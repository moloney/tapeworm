import os, logging, random, tarfile, shutil, errno, copy, operator, time
from os import path
from datetime import datetime, timedelta
from contextlib import contextmanager
from gzip import GzipFile

from pathmap import PathMap, warn_on_error
import peewee as pw

from util import (DatabaseModel, 
                  sp_exec, 
                  NonZeroReturnException, 
                  get_free_space,
                  total_seconds)
from tapemgr import FileReference, TapeIndex, Tape
from par import ParFactory


logger = logging.getLogger(__name__)


def path_to_ordinal(in_path):
    '''Convert a path to an ordinal. The `in_path` should not have leading 
    or trailing slash.'''
    toks = in_path.split(os.sep)
    toks.insert(0, len(toks))
    return toks


DATE_FMT = '%Y%m%d_%H%M%S'
'''The format used to store datetimes as text.'''


class QueueState(DatabaseModel):
    '''The state of the work queue.

    Currently we just have one worker processing one job at a time, so we
    should just have one row.
    '''

    worker_pid = pw.IntegerField(null=True)
    '''The process ID of the current worker'''

    worker_state = pw.TextField(null=True)
    '''The line of output for the worker process when running
    "ps -o lstart,cmd". Uniquely identifies the process instance.'''


class QueueEntry(DatabaseModel):
    '''A queued work item.'''

    queued_date = pw.DateTimeField()
    '''The date the item was added to the queue'''

    priority = pw.FloatField(default=0.5)
    '''The priority of this item. Items with a higher priority will be
    processed first. Items with the same priority are processed in the order
    they were queued.'''

    available = pw.BooleanField(default=True)
    '''Whether this job is currently available to run.'''


class BackupJob(DatabaseModel):
    '''A single backup job. Can be run periodically.
    
    Uses a external file to store the backup state from the most recent 
    successful run.
    
    Provides the last backup date, the last seen date, the archive name for 
    the last backup, and the (relative) path for every file that existed 
    during the last backup (or was seen recently).

    The data is gzipped text with each line giving the values for a single 
    path using BACKUP_LINE_DELIM as the delimiter. The dates are formatted 
    with DATE_FMT.
    '''
    
    name = pw.CharField(unique=True)
    '''The name of the backup job'''
    
    def index_dir(self, base_dir):
        '''Path where indices are stored for this job'''
        return path.join(base_dir, self.name)
    
    def last_state_path(self, base_dir):
        '''Return the path for the last backup state file.'''
        return path.join(self.index_dir(base_dir), 'last_state.gz')
        
    def new_state_path(self, base_dir):
        '''Return the path used for a new backup state file while it is being 
        created. Once the update is successful this should be renamed to the 
        `last_state_path`.'''
        return path.join(self.index_dir(base_dir), 'new_state.gz')
        
    def gen_state(self, base_dir):
        '''Generate the contents of the last backup state. For each line it 
        will yield the string tokens, the backup datetime, the last seen 
        datetime, and the ordinal for the path.
        
        The tokens will be the string represention of the last backup 
        date/time, last seen date/time, the archive name, and the path.
        '''
        with open(self.last_state_path(base_dir)) as state_f:
            state = GzipFile(fileobj=state_f)
            for line in state:
                line = line.strip()
                toks = line.split('\t')
                bu_dt = datetime.strptime(toks[0], DATE_FMT)
                seen_dt = datetime.strptime(toks[1], DATE_FMT)
                yield (toks, bu_dt, seen_dt, path_to_ordinal(toks[3]))

    @classmethod
    def toks_to_line(cls, toks):
        '''Convert string tokes to a line of text ready to be written to the 
        backup state file.'''
        return '\t'.join(toks) + '\n'
    
    @classmethod
    def encode_state(cls, bu_dt, seen_dt, arc_name, pth):
        '''Create a line of text encoding the given backup state information.
        '''
        toks = [bu_dt.strftime(DATE_FMT),
                seen_dt.strftime(DATE_FMT),
                arc_name,
                pth]
        return cls.toks_to_line(toks)
    

class BackupRun(DatabaseModel):
    '''A single run of a backup job. Can produce multiple archives.'''

    job = pw.ForeignKeyField(BackupJob, related_name='backup_runs')
    '''The backup job we are running'''
    
    root_dir = pw.TextField()
    '''The root directory we are backing up.'''

    queue_entry = pw.ForeignKeyField(QueueEntry,
                                     null=True,
                                     related_name='backup_run',
                                     on_delete='SET NULL')
    '''If this has not run yet we will have a reference to a queue entry'''

    run_date = pw.DateTimeField(null=True)
    '''The date of the run'''

    successful = pw.BooleanField(default=False)
    '''Whether the run completed successfully'''
    
    def index_dir(self, base_dir):
        return path.join(self.job.index_dir(base_dir),
                         self.run_date.strftime(DATE_FMT))


class ParArchive(DatabaseModel):
    '''A par2 archive which provides parity data for an archive'''

    file_ref = pw.ForeignKeyField(FileReference, related_name='pararchive')
    '''A reference to the file which is indexed onto tape.'''


class Archive(DatabaseModel):
    '''A single archive which contains one or more files.'''

    file_ref = pw.ForeignKeyField(FileReference, related_name='archive')
    '''A reference to the file which is indexed onto tape.'''

    first_path = pw.TextField()
    '''The first path stored in this archive'''

    last_path = pw.TextField()
    '''The last path stored in this archive'''

    run = pw.ForeignKeyField(BackupRun, related_name='archives')
    '''The backup job run that created this archive'''
    
    run_index = pw.IntegerField()
    '''Order the files associated with a BackupRun'''

    pararchive = pw.ForeignKeyField(ParArchive, 'archive', null=True)
    '''An optional pararchive which provides parity data.'''
    
    def index_path(self, base_dir, archive_fn=None):
        '''Return the path to the gzipped index file for this archive. If 
        the `file_ref` has not been set yet the `archive_fn` argument must 
        be provided.'''
        if archive_fn is None:
            archive_fn = self.file_ref.name
        basename = archive_fn.split('.')[0]
        return path.join(self.run.index_dir(base_dir), basename + '.gz')
                         
    def contents(self, base_dir):
        '''Generator produces the content indices for this archive. Yields a 
        tuple containing the size, modified datetime, and path.'''
        with open(self.index_path(base_dir)) as idx_f:
            indices = GzipFile(fileobj=idx_f)
            for line in indices:
                line = line.strip()
                toks = line.split('\t')
                yield (int(toks[0]), 
                       datetime.strptime(toks[1], DATE_FMT),
                       toks[2])
                       
    @classmethod
    def toks_to_line(cls, toks):
        '''Convert string tokes to a line of text ready to be written to the 
        backup state file.'''
        return '\t'.join(toks) + '\n'
    
    @classmethod
    def encode_index(cls, size, m_time, pth):
        '''Create a line of text encoding the index information.
        '''
        return cls.toks_to_line((str(size), m_time.strftime(DATE_FMT), pth))
        

class RestoreRun(DatabaseModel):
    '''A data restoration run. Used to keep track of queued work items.'''

    queue_entry = pw.ForeignKeyField(QueueEntry, related_name='restore_run')
    '''The associated entry in the work queue'''
    
    dest_dir = pw.TextField()
    '''The path to the directory we are restoring data to'''
    
    min_free_space = pw.BigIntegerField(default=0)
    '''The minimum amount of free space on the destination device. The 
    restoration will pause while this constraint is not met.'''
    
    strip_archive = pw.BooleanField(default=False)
    '''If true the the archive name will be stripped from the files being 
    restored. If any paths collide, only the newest one will be stored.'''
    
    is_complete = pw.BooleanField(default=False)
    '''Will be true when all of the associated requests have been created.'''
    
    full_archives = pw.BooleanField(default=False)
    '''Copy the full archives off tape rather than extracting files. 
    Mutually exclusive to the `strip_archive` option.'''
    
    with_pararchives = pw.BooleanField(default=False)
    '''Also copy any associated pararchives. Only valid when `full_archives`
    is set to true.'''
    
    def index_dir(self, base_dir):
        return path.join(base_dir, 
                         'restore',
                         self.queue_entry.queued_date.strftime(DATE_FMT))


class RestoreRequest(DatabaseModel):
    '''Specify an the contents of a single archive needed by a `RestoreRun`.
    '''

    run = pw.ForeignKeyField(RestoreRun, related_name='sub_requests')
    '''The RestoreRun depending on this request'''

    archive = pw.ForeignKeyField(Archive, related_name='restore_requests')
    '''The archive being requested'''

    first_path = pw.TextField(null=True)
    '''The first path (BFS order) to restore from the archive.'''

    def index_path(self, base_dir):
        '''The path to the index file for this request'''
        base_name = self.archive.file_ref.name.split('.')[0]
        return path.join(self.run.index_dir(base_dir), base_name)
        
    def contents(self, base_dir):
        '''Produces the relative paths to restore from this archive.'''
        first_ord = path_to_ordinal(self.first_path)
        with open(self.index_path(base_dir)) as idx_f:
            for line in idx_f:
                line = line.strip()
                rel_path = line.split('\t')[-1]
                rel_ord = path_to_ordinal(rel_path)
                if rel_ord < first_ord:
                    continue
                yield rel_path
                         

class CopyRun(DatabaseModel):
    queue_entry = pw.ForeignKeyField(QueueEntry, related_name='copy_run')
    '''The associated entry in the work queue'''
    
    dest_dir = pw.TextField()
    '''The path to the directory we are restoring data to'''
    
    min_free_space = pw.BigIntegerField(default=0)
    '''The minimum amount of free space on the destination device. The 
    restoration will pause while this constraint is not met.'''
    
    with_pararchives = pw.BooleanField(default=False)
    '''Also copy any associated pararchives. Only valid when `full_archives`
    is set to true.'''
    
    is_complete = pw.BooleanField(default=False)
    '''Will be true when all of the associated requests have been created.'''
    

class CopyRequest(DatabaseModel):
    run = pw.ForeignKeyField(CopyRun, related_name='sub_requests')
    '''The CopyRun depending on this request'''
    
    file_ref = pw.ForeignKeyField(FileReference, related_name='copy_requests')
    '''The file requested to copy off tape.'''


class BackupJobSpec(object):
    '''Captures various dynamic parameters associated with a backup job.

    Parameters
    ----------
    name : str
        The name of the backup job we are providing the specs for

    tape_sets : tuple
        The names of the tape sets we are backing up to

    pathmap : PathMap
        Controls which paths under the root directory are backed up. If None 
        all paths are backed up.

    periodic_range : tuple of timedelta
        Gives the minimum and maximum time range for periodic backups. If the
        time since the last backup for a file exceeds the minimum time it
        becomes eligible for a periodic backup. A random selection of the
        eligible files is backed up on each run, proportional to the time
        since the last backup run and length of the periodic range. Files
        that exceed the maximum are always backed up, but statistically this
        should be uncommon.

    par_percent : float
        The percent of parity to calculate and store for each archive. Will
        compute the parity using the 'par2' command. The resulting files will
        be stored in their own (uncompressed) tar archive.

    max_missing_age : timedelta
        The maximum amount of time to keep track of files that have gone
        missing in the main index. Files that are older than this can still
        be found, but it requires searching through the archive indices.
    '''

    def __init__(self, name, tape_sets, pathmap=None, 
                 periodic_range=None, par_percent=None,
                 max_missing_age=timedelta(days=180)):
        self.name = name
        self.tape_sets = tape_sets
        self.pathmap = pathmap
        if self.pathmap is None:
            self.pathmap = PathMap()
        self.pathmap.on_error = warn_on_error
        self.pathmap.sort = True
        self.periodic_range = periodic_range
        self.par_percent = par_percent
        self.max_missing_age = max_missing_age

    def backup_matches(self, root_dir, index_dir, current_run_dt, last_run_dt):
        '''Coroutine that generates the match results and relative paths for 
        paths that should be backed up on this run while also creating an 
        updated backup state for the job. The name of archive must be sent to 
        the coroutine once at the beginning and any time the archive changes.
        
        Replacing the old backup state file with the new one must be done 
        elsewhere, once the it is known the run completed successfully 
        (all archives flushed to tape).

        Parameters
        ----------
        root_dir : str
            The root directory we are backing up
        
        index_dir : str
            The base directory all index files are stored under

        current_run_dt : datetime
            The date and time for the current run

        last_run_dt : datetime or None
            The date and time of the last successful run, or None.
        '''
        root_dir = os.path.abspath(root_dir)
        
        # Look up the job in the database
        job = BackupJob.select().where(BackupJob.name == self.name).get()
        
        # Do some preprocessing
        curr_run_dt_str = current_run_dt.strftime(DATE_FMT)
        if self.periodic_range is not None:
            if last_run_dt is not None:
                time_since_last = current_run_dt - last_run_dt
            else:
                time_since_last = timedelta(0)
            periodic_len = (self.periodic_range[1] - self.periodic_range[0])
            periodid_len_sq = total_seconds(periodic_len) ** 2
            # Determine the maximum probability that a file is selected
            # for a periodic backup. Between the minimum and maximum of the
            # periodic range we ramp the probability from zero to this
            # value based on the amount of time since the last backup of the
            # file (using an X^2 probability density function). The factor of
            # eleven is a heuristic that does a good job of avoiding a large
            # spike in backup activity from files hitting the maximum limit
            # of the periodic range (see sim_periodic.py).
            max_rand_select_prob = (total_seconds(time_since_last) /
                                    total_seconds(periodic_len)
                                   ) * 11

        # Setup the last backup state generator and get the first entry
        if last_run_dt is None:
            last_backup_gen = (x for x in tuple())
        else:
            last_backup_gen = job.gen_state(index_dir)
        try:
            last_toks, last_bu_dt, last_seen_dt, last_ord = \
                next(last_backup_gen)
            last_path = last_toks[-1]
        except StopIteration:
            last_path = None
            
        # Start the pathmap generator. Skip the empty first result from the 
        # root dir itself and then grab the next result
        match_gen = self.pathmap.matches(root_dir)
        match_gen.next()
        root_len = len(root_dir) + 1
        try:
            match = match_gen.next()
            rel_path = match.path[root_len:]
        except StopIteration:
            match = None
            rel_path = None
            
        # Yield None when the coroutine is primed, then get the name of 
        # the first archive (yielding None again to the send call)
        arc_name = yield None
        assert arc_name is not None
        yield None
        
        # Setup file for writing new backup state, if any errors occur we 
        # will delete it.
        new_state_path = job.new_state_path(index_dir)
        new_backup_f = GzipFile(new_state_path, mode='w')
        try:
            # Loop through keeping the matches and last backup info in sync
            while match is not None or last_path is not None:
                if rel_path == last_path:
                    # The matched path has last backup info, check if it 
                    # should be backed up on this run
                    do_backup = False
                    st = match.dir_entry.stat()
                    m_time = datetime.fromtimestamp(st.st_mtime)
                    if last_bu_dt < m_time:
                        # Backup files that have been modified since the last
                        # time they were backed up but not since the beginning 
                        # of this backup run (these will be picked up on the 
                        # next pass to avoid duplication and allow indexing by
                        # modification time).
                        do_backup = m_time < current_run_dt
                    elif self.periodic_range is not None:
                        # Otherwise we may still do a periodic backup
                        time_since_last = current_run_dt - last_bu_dt
                        if time_since_last > self.periodic_range[1]:
                            # If we have exceeded the max periodic range we
                            # definitely do another backup
                            do_backup = True
                        elif time_since_last > self.periodic_range[0]:
                            # Otherwise, provided we are are above the min
                            # periodic range, we randomly select a subset of 
                            # the data to backup.
                            prob = ((total_seconds(time_since_last) ** 2) /
                                    periodic_len_sq) * max_rand_select_prob
                            do_backup = random.random() < prob

                    if do_backup:
                        # Yield the result, checking if the archive name has 
                        # changed before we write the new backup state
                        new_archive = yield (match, rel_path)
                        if new_archive:
                            arc_name = new_archive
                            yield None # Yield None to the send call
                        new_line = BackupJob.toks_to_line((curr_run_dt_str,
                                                           curr_run_dt_str,
                                                           arc_name,
                                                           rel_path)
                                                         )
                    else:
                        # Just update the last seen date/time
                        new_line = BackupJob.toks_to_line((last_toks[0],
                                                           curr_run_dt_str,
                                                           last_toks[2],
                                                           rel_path)
                                                         )
                    new_backup_f.write(new_line)

                    # Update both the current match and the last backup info
                    try:
                        match = match_gen.next()
                        rel_path = match.path[root_len:]
                    except StopIteration:
                        match = None
                        rel_path = None
                    try:
                        last_toks, last_bu_dt, last_seen_dt, last_ord = \
                            next(last_backup_gen)
                        last_path = last_toks[-1]
                    except StopIteration:
                        last_path = None

                else:
                    # The matched path and last backup info are out of sync, 
                    # either the former is new or the latter has been removed 
                    # (or both).
                    
                    # Figure out BFS ordering for the matched path
                    if match is not None:
                        match_ord = path_to_ordinal(rel_path)

                    if (last_path is None or 
                        (match is not None and match_ord < last_ord)
                       ):
                        # The matched path is new so we yield it and then add 
                        # it to the backup state
                        new_archive = yield (match, rel_path)
                        if new_archive:
                            arc_name = new_archive
                            yield None # Yield None to the send call
                        new_line = BackupJob.toks_to_line((curr_run_dt_str,
                                                           curr_run_dt_str,
                                                           arc_name,
                                                           rel_path)
                                                         )
                        new_backup_f.write(new_line)

                        # Update the current match
                        try:
                            match = match_gen.next()
                            rel_path = match.path[root_len:]
                        except StopIteration:
                            match = None
                            rel_path = None
                    else:
                        # The last backup info is for a file that no longer 
                        # exists. Walk through last backup lines until we 
                        # catch up
                        while (last_path is not None and 
                               (match is None or last_ord < match_ord)):
                            # Check if we should drop this file from the last 
                            # backup state
                            missing_age = current_run_dt - last_seen_dt
                            if (self.max_missing_age is None or 
                                missing_age < self.max_missing_age
                               ):
                                line = BackupJob.toks_to_line(last_toks)
                                new_backup_f.write(line)

                            # Update the last backup info
                            try:
                                (last_toks, 
                                 last_bu_dt, 
                                 last_seen_dt, 
                                 last_ord) =  next(last_backup_gen)
                                last_path = last_toks[-1]
                            except StopIteration:
                                last_path = None
        except BaseException:
            new_backup_f.close()
            os.remove(new_state_path)
            raise
        new_backup_f.close()


class OverSizeFileError(Exception):
    '''Raised if a single file exceeds the hard limit on chunk size.'''
    def __init__(self, file_path):
        self.file_path = file_path

    def __str__(self):
        return 'File exceeds chunk size: %s' % file_path


def gen_archives(match_results, max_size, hard_limit=True):
    ''''Coroutine that receives a tuple containing a writable TarFile for the 
    file data, a writable file-like for index data, and a prefix string to be 
    added to the relative paths in the tar archive. Each time this tuple is 
    recieved some portion of the generator `match_results` will be processed. 
    The first and last path processed will be yielded to each `send` call.

    If `match_results` is empty will raise StopIteration when the coroutine
    is primed.

    The size of each archive will be less than `max_size`. If a `hard_limit`
    is False, single files that exceed the maximum chunk size will be put
    into a single file archive rather than raising an OverSizeFileError.

    Parameters
    ----------
    match_results : generator of pathmap.MatchResult
        Generates a match result for each path that should be archived

    max_size : int
        The maximum size in bytes for any generated tar file.

    hard_limit : bool
        Individual files that exceed `max_size` go into single file archives
        rather than raise an exception.
        
    Returns
    -------
    first_last_paths : tuple
        The first and last path included in the last archive.
    '''

    n_files = 0
    curr_tf = None
    first_path, last_path = (None, None)

    for (match_result, rel_pth) in match_results:
        if curr_tf is None:
            curr_tf, index_file, prefix = yield None
        tf_size = curr_tf.fileobj.tell()
        st = match_result.dir_entry.stat()
        f_size = st.st_size
        pth = match_result.path
        if max_size - tf_size < f_size:
            if n_files == 0 and hard_limit:
                raise OverSizeFileError(pth)
            else:
                logger.info("Wrote %d files to the archive", n_files)
                curr_tf, index_file, prefix = yield (first_path, last_path)
                n_files = 0

        t_info = curr_tf.gettarinfo(pth, arcname=prefix+rel_pth)
        if match_result.dir_entry.is_dir():
            curr_tf.addfile(t_info)
        else:
            with open(match_result.path) as f_obj:
                curr_tf.addfile(t_info, f_obj)
        if first_path is None:
            first_path = rel_pth
        last_path = rel_pth
        if index_file is not None:
            mt = datetime.fromtimestamp(st.st_mtime)
            idx_line = Archive.encode_index(f_size, mt, rel_pth)
            index_file.write(idx_line)
        n_files += 1

    if curr_tf is not None:
        logger.info("Wrote %d files to the archive", n_files)
        yield (first_path, last_path)
        
        
def get_process_state(pid):
    '''Get additional identifying information for the process with the given
    `pid`.

    Provides a sting with the start time and command line. Combined with the
    pid this does a reasonably good job uniquely identifying a specific
    instance of a process.
    '''
    try:
        stdout, _ = sp_exec(['ps', '--pid', str(pid), '-o', 'lstart,cmd'])
        lines = [x.strip() for x in stdout.split('\n') if x != '']
        assert len(lines) == 2
        return lines[1]
    except NonZeroReturnException:
        return ''


class ProcessLockConflict(Exception):
    def __init__(self, curr_worker_pid):
        self.curr_worker_pid = curr_worker_pid

    def __str__(self):
        return 'Lock is already held by process %d' % self.curr_worker_pid


def _needs_process_lock(wrapped):
    def wrapper(self, *args, **kwargs):
        if not self._has_lock:
            raise ValueError("A processing lock is required by this method")
        return wrapped(self, *args, **kwargs)
    return wrapper


class JobManager(object):
    def __init__(self, spool, job_specs, database, index_dir, init_db=False,
                 archive_size_limit=80*(1024**3), 
                 report_full_freq=timedelta(weeks=1)):
        self.spool = spool
        self._job_specs = {}
        for js in job_specs:
            self._job_specs[js.name] = js
        self.database = database
        self.index_dir = index_dir
        self.archive_size_limit = archive_size_limit
        self.report_full_freq = report_full_freq
        self._has_lock = False

        # Create a ParFactory to handle the creation of any par2 archives, 
        # plus a dict to track pararchives that have not completed
        self.par_factory = ParFactory()
        self._outstanding_pararchives = {}

        # Set the callback for flushed writes plus some dicts to track
        # outstanding writes
        self.spool.set_flush_callbacks(self._pre_flush_callback,
                                       self._post_flush_callback)
        self._spooled_archives = {}
        self._spooled_pararchives = {}
        self._spooled_runs = {}

        # Initialize the database if requested
        if init_db:
            QueueState.create_table()
            QueueState.create(worker_pid=None)
            QueueEntry.create_table()
            BackupJob.create_table()
            BackupRun.create_table()
            ParArchive.create_table()
            Archive.create_table()
            RestoreRun.create_table()
            RestoreRequest.create_table()
            CopyRun.create_table()
            CopyRequest.create_table()
            os.mkdir(path.join(self.index_dir, 'restore'))

        # Add any missing backup jobs to the database
        for job_spec in job_specs:
            try:
                job = BackupJob.get(BackupJob.name == job_spec.name)
            except BackupJob.DoesNotExist:
                job = BackupJob.create(name=job_spec.name)
                job_dir = job.index_dir(self.index_dir)
                os.mkdir(job_dir)

    @contextmanager
    def processing_lock(self):
        '''Context manager that provides exclusive access to underlying
        hardware in order to process work queue entries.'''
        pid = os.getpid()
        logger.info("Trying to make process %d the worker", pid)
        with self.database.granular_transaction('exclusive'):
            queue_state = QueueState.get()
            if queue_state.worker_pid is not None:
                # Check if the registered worker process is still around
                if (queue_state.worker_state !=
                    get_process_state(queue_state.worker_pid)
                   ):
                    logger.warning("Previous worker %d did not release lock",
                                    queue_state.worker_pid)
                else:
                    raise ProcessingLockConflict(queue_state.worker_pid)
            queue_state.worker_pid = pid
            queue_state.worker_state = get_process_state(pid)
            queue_state.save()
        self._has_lock = True
        logger.info("Successfully made process %d the worker", pid)

        try:
            yield
        except BaseException:
            raise
        finally:
            logger.info("Releasing worker lock for process %d", pid)
            self._has_lock = False
            with self.database.granular_transaction('exclusive'):
                queue_state = QueueState.get()
                assert queue_state.worker_pid == pid
                queue_state.worker_pid = None
                queue_state.worker_state = None
                queue_state.save()
            logger.info("Successfully released worker lock for process %d",
                         pid)

    def _write_pararchives(self, path_sets):
        for path_set in path_sets:
            hdr_path = path_set[0]
            par_file_ref, archive, tape_sets = \
                self._outstanding_pararchives[hdr_path]
            del self._outstanding_pararchives[hdr_path]
            par_file_ref.size = sum(os.stat(p).st_size 
                                    for p in path_set)
            with self.spool.write(tape_sets, 
                                  par_file_ref, 
                                  size_is_approx=True) as dest_f:
                dest_tf = tarfile.open(fileobj=dest_f, mode='w')
                for p in path_set:
                    dest_tf.add(p, 
                                arcname=p.split(os.sep)[-1], 
                                recursive=False)
                dest_tf.close()
            self._spooled_pararchives[par_file_ref] = archive
            for p in path_set:
                os.remove(p)


    def _do_backup_run(self, run):
        '''Run a backup job.'''
        # Find the job_spec
        job_spec = self._job_specs[run.job.name]
        logger.info("Starting backup for job %s" % job_spec.name)

        # Look up the job and the last successful run in the database
        job = run.job
        try:
            last_run = BackupRun.select().\
                where((BackupRun.job == job) & (BackupRun.successful == True)).\
                order_by(BackupRun.run_date.desc()).get()
            last_run_dt = last_run.run_date
            logger.info("Last successful run was %s", last_run.run_date)
        except BackupRun.DoesNotExist:
            last_run_dt = None
            logger.info("No previous successful runs found for this job")

        # Create a new run with a directory for archive indices. We get rid 
        # of any datetime component more fine grained than one second so that 
        # there is no precision loss going to a string representation and back
        new_run_dt = datetime.now().replace(microsecond=0)
        new_run_dt_str = new_run_dt.strftime("%Y%m%d_%H%M%S")
        run.run_date = new_run_dt
        run.save()
        run_dir = path.join('backup', job.name, new_run_dt_str)
        os.mkdir(run.index_dir(self.index_dir))
        
        # Create first archive name
        arc_fn_fmt = '-'.join([job_spec.name, new_run_dt_str, '%03d'])
        archive_idx = 0
        arc_basename = arc_fn_fmt % archive_idx
        archive_name = arc_basename + '.tar.gz'
        
        # Setup the coroutine that produces the paths we are going to 
        # backup, while at the same time writing the new backup state. We 
        # need to send it the archive name so it can be stored in the backup
        # state.
        match_gen = job_spec.backup_matches(run.root_dir,
                                            self.index_dir, 
                                            new_run_dt, 
                                            last_run_dt)
        next(match_gen)
        match_gen.send(archive_name)

        # Setup the coroutine to generate tar archives within the size
        # constraint
        arc_gen = gen_archives(match_gen, self.archive_size_limit)
        has_files = True
        try:
            next(arc_gen)
        except StopIteration:
            logger.info("No paths to backup for this job run")
            has_files = False

        # Iteratively create archives for this backup job
        last_file = None
        while has_files:
            # Update the archive info
            if archive_idx != 0:
                arc_basename = arc_fn_fmt % archive_idx
                archive_name = arc_basename + '.tar.gz'
                try:
                    # Tell the backup_matches coroutine the archive name so 
                    # it can be saved in the backup state
                    match_gen.send(archive_name)
                except StopIteration:
                    pass 
            logger.info("Creating archive %s", archive_name)
            archive_idx += 1
            
            # Create ORM objects but don't add them to the DB yet
            file_ref = FileReference(name=archive_name,
                                     size=self.archive_size_limit)
            archive = Archive(run=run, run_index=archive_idx)
            
            # Create a file for the archive index data, if any error 
            # occurs we delete this file.
            idx_path = archive.index_path(self.index_dir, archive_name)
            idx_f = GzipFile(idx_path, mode='w')
            try:
                # Create a gzipped tar file on the spool, the spool will 
                # delete the file if any error occurs
                with self.spool.write(job_spec.tape_sets,
                                      file_ref,
                                      size_is_approx=True) as data_f:
                    gz_f = GzipFile(fileobj=data_f, compresslevel=6)
                    data_tf = tarfile.open(fileobj=gz_f, mode='w')
                    try:
                        first_path, last_path = \
                            arc_gen.send((data_tf, 
                                          idx_f,
                                          arc_basename+'/')
                                        )
                    finally:
                        # Need to close the tarfile before the file object
                        data_tf.close()
                        gz_f.close()
                        
            except BaseException, e:
                idx_f.close()
                os.remove(idx_path)
                
                if isinstance(e, StopIteration):
                    has_files = False
                else:
                    raise
            else:
                idx_f.close()

            if has_files:
                last_file = file_ref
                # Fill out remaining archive information, but don't save it 
                # until we know the file was flushed to tape
                archive.first_path = first_path
                archive.last_path = last_path
                self._spooled_archives[file_ref] = archive
                
                # Queue up the creation of a par archive if needed
                if job_spec.par_percent is not None:
                    sp_path = self.spool.get_spooled_path(file_ref)
                    basename = file_ref.name.split('.')[0] 
                    par_file_ref = FileReference(name=basename + '.par2.tar')
                    last_file = par_file_ref
                    hdr_path = self.par_factory.queue(sp_path, 
                                                      basename, 
                                                      job_spec.par_percent)
                    self._outstanding_pararchives[hdr_path] = \
                        (par_file_ref, archive, job_spec.tape_sets)

            # If any outstanding pararchives have completed we write them to 
            # the spool as a tar archive
            if self.par_factory.n_done != 0:
                self._write_pararchives(self.par_factory.get_completed())
                
        if last_file is not None:
            # Update but don't save the run until we know all the 
            # related files have been flushed to tape (see _flushed_write_cb)
            self._spooled_runs[last_file] = run
        else:
            # No files were written but the backup state may have still been 
            # updated (i.e. dropping missing files)
            os.rename(job.new_state_path(self.index_dir),
                      job.last_state_path(self.index_dir))
            run.successful = True
            run.save()

        logger.info("Finished processing backup job %s" % job_spec.name)

    def _pre_flush_callback(self):
        # Wait for any outstanding pararchives to finish before the spool is 
        # flushed
        while self.par_factory.n_queued != 0:
            time.sleep(5)
    
    def _post_flush_callback(self):
        # Delete the index file for any archives that didn't make it to tape. 
        # If the archive made it to any tapes we save it in the database
        for file_ref, archive in self._spooled_archives.iteritems():
            if file_ref.id is None:
                os.remove(archive.index_path(self.index_dir, file_ref.name))
            else:
                archive.file_ref = file_ref
                archive.save()
        self._spooled_archives.clear()
        
        # Save pararchives that made it to any tapes
        for file_ref, arc in self._spooled_pararchives.iteritems():
            if file_ref.id is not None:
                pararc = ParArchive.create(file_ref=file_ref)
                arc.pararchive = pararc
                arc.save()
        self._spooled_pararchives.clear()
        
        # Delete the new backup state file for any job runs that didn't finish
        # and mark any runs that did finish as successful (make sure the last 
        # file made it to ALL of the tape sets).
        completed = []
        for file_ref, run in self._spooled_runs.iteritems():
            job_spec = self._job_specs[run.job.name]
            
            # If the last file for the run was a pararchive, it may not have 
            # completed and been spooled yet and thus we need to continue 
            # waiting for it
            # TODO: Is hashing model instances ok if we need to use 'id' for comparisons?
            if any(id(par_file_ref) == id(file_ref)
                   for (par_file_ref, _, _) in self._outstanding_pararchives.itervalues()):
                continue
            
            # Need to see if the file made it to all tape sets
            run.successful = True
            if file_ref.id is not None:
                written_sets = set(x.tape.tape_set.name 
                                   for x in TapeIndex.select().\
                                    where(TapeIndex.file_ref == file_ref)
                                  )
                if written_sets != set(job_spec.tape_sets):
                    run.successful = False
            else:
                run.successful = False
            
            if run.successful:
                os.rename(run.job.new_state_path(self.index_dir),
                          run.job.last_state_path(self.index_dir))
                run.save()
                logger.info("Successfully completed run for job %s", 
                             run.job.name)
            else:
                os.remove(run.job.new_state_path(self.index_dir))
            completed.append(file_ref)
        for file_ref in completed:
            del self._spooled_runs[file_ref]
        
        # Write out any pararchives that completed in the pre flush callback
        self._write_pararchives(self.par_factory.get_completed())

    def _do_restore_run(self, run):
        '''Run as much of the restore job as possible with the available
        tapes.'''
        # TODO: If we only complete part of a request in one run due to 
        # running out of space, the  directory m_time values will be off 
        # after the next run. Another possible concern would be extracting 
        # directories that we don't have write permissions for. These 
        # concerns also apply, even if we do process full requests, if we are 
        # stripping the archive prefix during the restore (since we split up 
        # a single directory tree into multiple archives). 
        restore_dt_str = run.queue_entry.queued_date.strftime('%Y%m%d_%H%M%S') 
        logger.info("Starting restore run that was queued %s", restore_dt_str)
                     
        # Restore archives from newer backup runs first and process the 
        # archives from each run in the order they were written
        remaining_requests = 0
        completed_requests = []
        avail_tapes = self.spool.tape_mgr.changer.tapes
        out_of_space = False
        dest_free = get_free_space(run.dest_dir)
        for req in RestoreRequest.select().\
                    where(RestoreRequest.run == run).\
                    join(Archive).\
                    join(BackupRun).\
                    order_by(BackupRun.run_date.desc(), 
                             Archive.run_index):

            # Keep track of requests we skip so we know when we are done
            if not any(tape_idx.tape.barcode in avail_tapes
                       for tape_idx in req.archive.file_ref.tape_indices):
                remaining_requests += 1
                continue
                
            logger.info("Restoring data from archive %s", 
                        req.archive.file_ref.name)
            
            # Copy the file to the spool and then open it as a tar file
            arc_basename = req.archive.file_ref.name.split('.')[0]
            prefix = arc_basename + '/'
            with self.spool.read(req.archive.file_ref) as src_f:
                src_gz = GzipFile(fileobj=src_f)
                src_tf = tarfile.TarFile(fileobj=src_gz)
                directories = []
                last_path = None
                for req_path in req.contents(self.index_dir):
                    member_name = str(prefix + req_path).strip()
                    member_ord = path_to_ordinal(member_name)
                    t_info = next(src_tf)
                    while os.path.normpath(t_info.name) != member_name:
                        t_info = next(src_tf)
                        if (path_to_ordinal(os.path.normpath(t_info.name)) >
                            member_ord
                           ):
                            raise KeyError("Unable to find: %s" % member_name)
                    if (dest_free - t_info.size < run.min_free_space):
                        req.first_path = req_path
                        req.save()
                        out_of_space = True
                        break
                    dest_free -= t_info.size
                    try:
                        if run.strip_archive:
                            t_info.name = t_info.name[len(prefix):]
                        if t_info.isdir():
                            # Extract directories with a safe mode.
                            directories.append(t_info)
                            t_info = copy.copy(t_info)
                            t_info.mode = 0700
                            
                        # If the path already exists with a equal or newer 
                        # m_time we skip it
                        dest_path = path.join(run.dest_dir, t_info.name)
                        if path.exists(dest_path):
                            if t_info.mtime <= path.getmtime(dest_path):
                                continue
                        
                        # Do the extraction for this file
                        src_tf.extract(t_info, run.dest_dir)
                    except BaseException, e:
                        if last_path is not None:
                            req.first_path = last_path
                            req.save()
                        if hasattr(e, 'errno') and e.errno == errno.ENOSPC:
                            out_of_space = True
                            break
                        else:
                            raise
                    last_path = req_path
                
                # Reverse sort directories.
                directories.sort(key=operator.attrgetter('name'))
                directories.reverse()

                # Set correct owner, mtime and filemode on directories.
                for tarinfo in directories:
                    dirpath = path.join(run.dest_dir, tarinfo.name)
                    try:
                        src_tf.chown(tarinfo, dirpath)
                        src_tf.utime(tarinfo, dirpath)
                        src_tf.chmod(tarinfo, dirpath)
                    except ExtractError, e:
                        if src_tf.errorlevel > 1:
                            raise
                        else:
                            tfile._dbg(1, "tarfile: %s" % e)
            
            if out_of_space:
                break
            
            completed_requests.append(req)

        # Clean up the completed requests
        for req in completed_requests:
            os.remove(req.index_path(self.index_dir))
            req.delete_instance()

        if not out_of_space and remaining_requests == 0:
            # Clean up the completed run
            shutil.rmtree(run.index_dir(self.index_dir))
            with self.database.transaction():
                run.delete_instance()
                run.queue_entry.delete_instance()
            logger.info("Successfully finished a restore run")
        else:
            # Mark the run as unavailble until more of the needed tapes or 
            # enough free space are made available
            run.queue_entry.available = False
            run.queue_entry.save()
            logger.info("Completed %d requests", len(completed_requests))
            if out_of_space:
                logger.info("Not enough free space to complete run")
    
    def _do_copy_run(self, run):
        copy_dt_str = run.queue_entry.queued_date.strftime('%Y%m%d_%H%M%S') 
        logger.info("Starting copy run that was queued %s", copy_dt_str)
        
        remaining_requests = 0
        completed_requests = []
        avail_tapes = self.spool.tape_mgr.changer.tapes
        out_of_space = False
        for req in CopyRequest.select().\
                    where(CopyRequest.run == run):
            file_ref = req.file_ref
                                 
            # Keep track of requests we skip so we know when we are done
            if not any(tape_idx.tape.barcode in avail_tapes
                       for tape_idx in file_ref.tape_indices):
                remaining_requests += 1
                continue
                
            # Check if we have enough free space
            free_space = get_free_space(run.dest_dir) - run.min_free_space
            if (free_space < file_ref.size):
                out_of_space = True
                break
            
            logger.info("Copying archive %s", file_ref.name)
            dest_path = path.join(run.dest_dir, file_ref.name)
            tmp_dest_path = dest_path + '.tmp'
            with open(tmp_dest_path, 'w') as dest_f:
                with self.spool.read(file_ref) as src_f:
                    shutil.copyfileobj(src_f, dest_f)
            os.rename(tmp_dest_path, dest_path)
                    
            completed_requests.append(req)
        
        # Delete any completed requests
        for req in completed_requests:
            req.delete_instance()
            
        if not out_of_space and remaining_requests == 0:
            # Clean up the completed run
            with self.database.transaction():
                run.delete_instance()
                run.queue_entry.delete_instance()
            logger.info("Successfully finished a copy run")
        else:
            # Mark the run as unavailble until more of the needed tapes or 
            # enough free space are made available
            run.queue_entry.available = False
            run.queue_entry.save()
            logger.info("Completed %d requests", len(completed_requests))
            if out_of_space:
                logger.info("Not enough free space to complete run")
    
    def queue_backup(self, job, root_dir, priority=0.5):
        '''Queue a backup run for the given `jobs`.'''
        root_dir = path.abspath(root_dir)
        with self.database.granular_transaction('exclusive'):
            queue_entry = QueueEntry.create(queued_date=datetime.now(), 
                                            priority=priority)
            run = BackupRun.create(job=job, 
                                   root_dir=root_dir, 
                                   queue_entry=queue_entry)
            
    def _log_needed_tapes(self, tape_groups):
        req_missing = []
        req_reported_full = []
        avail_barcodes = self.spool.tape_mgr.changer.tapes
        for tape_group in tape_groups:
            avail = []
            for set_name, barcode in tape_group:
                if barcode in avail_barcodes:
                    avail.append(Tape.get(Tape.barcode == barcode))
            if len(avail) == 0:
                req_missing.append(tape_group)
            else:
                if all(t.reported_full_date is not None for t in avail):
                    req_reported_full.append(tape_group)
                    # Set the reported date back to None so only one message 
                    # is reported. Also ensures the tape is re-reported ASAP.
                    for tape in avail:
                        tape.reported_full_date = None
                        tape.save()
        log_lines = []
        if len(req_missing) != 0:
            log_lines.append("One tape from each of the following lines "
                             "needs to be loaded for data restoration:")
            for tape_group in req_missing:
                log_lines.append('\t' + 
                                 ', '.join('%s:%s' % t_info 
                                           for t_info in tape_group)
                                )
        if len(req_reported_full) != 0:
            log_lines.append("The following tapes were reported as full but are "
                             "now needed for data restoration, do NOT remove "
                             "until further notice:")
            for tape_group in req_reported_full:
                log_lines.append('\t' + 
                                 ', '.join('%s:%s' % t_info 
                                           for t_info in tape_group)
                                )
        if len(log_lines) != 0:
            logger.warn('\n'.join(log_lines))
        
    def queue_restore(self, contents, dest_dir, strip_archive=False, 
                      min_free_space=0, priority=0.5):
        '''Queue the restoration of the specfied `contents` to the `dest_dir`.
        '''
        with self.database.granular_transaction('exclusive'):
            # Create the queue entry and run in the DB, but mark them as not 
            # being ready/complete
            queue_entry = QueueEntry.create(queued_date=datetime.now(), 
                                            available=False,
                                            priority=priority)
            run = RestoreRun.create(queue_entry=queue_entry, 
                                    dest_dir=dest_dir,
                                    strip_archive=strip_archive,
                                    min_free_space=min_free_space,
                                    is_complete=False)
        
            # Make a directory for the index data
            idx_dir = run.index_dir(self.index_dir) 
            os.mkdir(idx_dir)
        
        # Keep track of the archive/request we are currently working with
        archive = None
        req_idx_f = None
        
        # Keep track of the groups of tapesets/tapes needed for this restore
        req_tape_groups = set()
        
        try:
            # Make all of the restore requests
            for line in contents:
                line = line.strip()
                toks = line.split('\t')
                archive_name, path = toks[-2:]
                
                # Check if this file is part of a different archive from the last
                if archive is None or archive_name != archive.file_ref.name:
                    archive = Archive.select().\
                                join(FileReference).\
                                where(FileReference.name == archive_name).\
                                get()
                    req_tape_group = \
                        frozenset((ti.tape.tape_set.name, ti.tape.barcode)
                                  for ti in archive.file_ref.tape_indices)
                    req_tape_groups.add(req_tape_group)
                    try:
                        request = RestoreRequest.select().\
                                    where((RestoreRequest.run == run) & 
                                          (RestoreRequest.archive == archive)).get()
                    except RestoreRequest.DoesNotExist:
                        request = RestoreRequest.create(run=run, 
                                                        archive=archive,
                                                        first_path=path
                                                       )
                    if req_idx_f is not None:
                        req_idx_f.close()
                    req_idx_f = open(request.index_path(self.index_dir), 'a')
                
                # Write the path to the index file
                req_idx_f.write(path+'\n')
                
            if req_idx_f is not None:
                req_idx_f.close()
        except BaseException:
            # Clean up on any exception
            with self.database.granular_transaction('exclusive'):
                for req in run.sub_requests:
                    req.delete_instance()
                run.delete_instance()
                queue_entry.delete_instance()
            shutil.rmtree(idx_dir)
            raise
        
        # Log the required tape groups where there is no tape currently 
        # present or the tapes that are present and were reported as full 
        # (so the user know not to remove that tape yet)
        self._log_needed_tapes(req_tape_groups)
        
        # Mark the run as complete. The availability will be updated as 
        # appropriate when the needed tapes (or free space) become available.
        run.is_complete = True
        run.save()
    
    def queue_copy(self, archives, dest_dir, with_pararchives=False, 
                   min_free_space=0, priority=0.5):
        '''Queue the copying of one or more `archives` to the `dest_dir`.'''
        with self.database.granular_transaction('exclusive'):
            # Create the queue entry and run in the DB, but mark them as not 
            # being ready/complete
            queue_entry = QueueEntry.create(queued_date=datetime.now(), 
                                            available=False,
                                            priority=priority)
            run = CopyRun.create(queue_entry=queue_entry, 
                                 dest_dir=dest_dir,
                                 min_free_space=min_free_space,
                                 with_pararchives=with_pararchives,
                                 is_complete=False)
        
        try:
            req_tape_groups = set()
            for arc_name in archives:
                file_ref = FileReference.select().\
                            where(FileReference.name == arc_name).\
                            get()
                archive = Archive.select().\
                            where(Archive.file_ref == file_ref).\
                            get()
                request = CopyRequest.create(run=run, file_ref=file_ref)
                req_tape_group = \
                            frozenset((ti.tape.tape_set.name, ti.tape.barcode)
                                      for ti in archive.file_ref.tape_indices)
                req_tape_groups.add(req_tape_group)
                if with_pararchives and archive.pararchive is not None:
                    pararc = archive.pararchive
                    request = CopyRequest.create(run=run, 
                                                 file_ref=pararc.file_ref)
                    par_tape_group = \
                            frozenset((ti.tape.tape_set.name, ti.tape.barcode)
                                      for ti in pararc.file_ref.tape_indices)
                    req_tape_groups.add(par_tape_group)
        except BaseException:
            # Clean up on any exception
            with self.database.granular_transaction('exclusive'):
                for req in run.sub_requests:
                    req.delete_instance()
                run.delete_instance()
                queue_entry.delete_instance()
            raise
        
        # Log the required tape groups where there is no tape currently 
        # present or the tapes that are present and were reported as full 
        # (so the user know not to remove that tape yet)
        self._log_needed_tapes(req_tape_groups)
        
        run.is_complete = True
        run.save()
    
    def get_run(self, queue_entry):
        '''Return the run associated this queue entry.'''
        try:
            return BackupRun.select().\
                    where(BackupRun.queue_entry == queue_entry).\
                    get()
        except BackupRun.DoesNotExist:
            try:
                return RestoreRun.select().\
                        where(RestoreRun.queue_entry == queue_entry).\
                        get()
            except RestoreRun.DoesNotExist:
                return CopyRun.select().\
                        where(RestoreRun.queue_entry == queue_entry).\
                        get()
                        
    def cancel_entry(self, queue_entry):
        '''Cancel the given entry in the work queue'''
        run = self.get_run(queue_entry)
        if isinstance(run, BackupRun):
            queue_entry.delete_instance()
            # If there are no associated archives, delete the run as well
            if len(list(run.archives)) == 0:
                run.delete_instance()
            else:
                run.queue_entry = None
        else:
            # Delete related requests first
            with self.database.transaction():
                sub_reqs = list(run.sub_requests)
                for sub_req in sub_reqs:
                    sub_req.delete_instance()
                run.delete_instance()
                queue_entry.delete_instance()
            
    
    @_needs_process_lock
    def process_work_queue(self):
        '''Process any jobs in the work queue, returning the number of jobs
        processed.

        A lock must be acquired using the `processing_lock` context manager
        before calling this method.
        '''
        # Initialize the spool if it hasn't been
        if not self.spool.is_init:
            self.spool.init()
        
        # See if any jobs that are currently marked unavailable should become  
        # available
        avail_tapes = self.spool.tape_mgr.changer.tapes
        dest_free = {}
        for queue_entry in QueueEntry.select().\
                            where(QueueEntry.available == False):
            run = self.get_run(queue_entry)
            assert not isinstance(run, BackupRun)
            if not run.is_complete:
                logging.info("Skipping incomplete run")
                continue
            
            # Check if the minimum amount of free space is available
            if not run.dest_dir in dest_free:
                dest_free[run.dest_dir] = get_free_space(run.dest_dir)
            if run.min_free_space > dest_free[run.dest_dir]:
                logger.info("Restore/Copy run to dest %s has insufficient "
                            "space.", run.dest_dir)
                continue
                    
            # Check if any of the needed tapes are available
            for req in run.sub_requests:
                if isinstance(run, RestoreRun):
                    tape_indices = req.archive.file_ref.tape_indices
                else:
                    tape_indices = req.file_ref.tape_indices
                for tape_idx in tape_indices:
                    if tape_idx.tape.barcode in avail_tapes:
                        queue_entry.available = True
                        queue_entry.save()
                        break
                if queue_entry.available:
                    break
        
        # TODO: Need to handle running out of blank tapes in a way that 
        # still allows restore/copy jobs to run
        # Loop selecting jobs from the queue and running them until the
        # queue is exhausted
        n_processed = 0
        while True:
            # Generate a list of the available runs, ordered by priority and 
            # then date
            runs = []
            with self.database.granular_transaction('exclusive'):
                for queue_entry in QueueEntry.select().\
                        where(QueueEntry.available == True).\
                        order_by(QueueEntry.priority.desc(), 
                                 QueueEntry.queued_date):
                    runs.append(self.get_run(queue_entry))
            
            if len(runs) == 0:
                break
            n_processed += len(runs)
            
            for run in runs:
                if isinstance(run, BackupRun):
                    # If it is a backup run we can just delete the queue 
                    # entry now since we can't do a partial run
                    run.queue_entry.delete_instance()
                    run.queue_entry = None
                    self._do_backup_run(run)
                elif isinstance(run, RestoreRun):
                    self._do_restore_run(run)
                else:
                    assert isinstance(run, CopyRun)
                    self._do_copy_run(run)

        # Flush the spool device
        self.spool.flush()
        
        # Check if there are any tapes that are full, not needed for a 
        # restore/copy, and haven't been reported recently
        avail_barcodes = self.spool.tape_mgr.changer.tapes
        for barcode in avail_barcodes:
            try:
                tape = Tape.get(Tape.barcode == barcode)
            except Tape.DoesNotExist:
                continue
            if tape.is_full:
                now = datetime.now()
                if (tape.reported_full_date is None or 
                    (now - tape.reported_full_date > self.report_full_freq)
                   ):
                    reqs = [x 
                            for x in RestoreRequest.select().\
                                join(Archive).\
                                join(FileReference).\
                                join(TapeIndex).\
                                where(TapeIndex.tape == tape)
                           ]
                    reqs += [x 
                             for x in CopyRequest.select().\
                                 join(FileReference).\
                                 join(TapeIndex).\
                                 where(TapeIndex.tape == tape)
                            ]
                    if len(reqs) == 0:
                        logger.warn("The tape '%s' from tape set '%s' can be "
                                    "removed", barcode, tape.tape_set.name)
                        tape.reported_full_date = now
                        tape.save()
                        
        # Reset the reported_full_date to None for any tape that is no longer 
        # in the changer
        with self.database.transaction():
            for tape in Tape.select().where(Tape.reported_full_date != None):
                if not tape.barcode in avail_barcodes:
                    tape.reported_full_date = None
                    tape.save()

        return n_processed

