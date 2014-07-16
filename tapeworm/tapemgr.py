import os, time, errno, logging, json
from datetime import datetime
from contextlib import contextmanager

import peewee as pw

from util import (DatabaseModel, 
                  UninitializedDatabaseError, 
                  get_readable_bw, 
                  total_seconds)
from tapeinfo import (UnhandledTapeAlertError,
                      CLEAN_ALERT_FLAGS,
                      WARN_ALERT_FLAGS,
                      EXPIRED_CLEAN_ALERT_FLAG,
                      IS_CLEAN_CART_ALERT_FLAG)
from devctrl import UnknownBlockSizeError


logger = logging.getLogger(__name__)


class TapeSet(DatabaseModel):
    '''A set of tapes.'''

    name = pw.CharField(unique=True)
    '''A unique name for the tape set.'''


class Tape(DatabaseModel):
    '''A single tape.'''

    barcode = pw.CharField(unique=True)
    '''Barcode that uniquely identifies the tape.'''

    tape_set = pw.ForeignKeyField(TapeSet, related_name='tapes')
    '''The tape set this tape belongs to.'''

    size = pw.BigIntegerField()
    '''The number of bytes this tape can hold.'''

    est_free_space = pw.BigIntegerField()
    '''The estimated amount of free space in bytes.'''

    next_block = pw.BigIntegerField()
    '''The block index for the next file to be written'''

    is_full = pw.BooleanField(default=False)
    '''Set to true when we are unable to complete a write to the tape due to
    limited free space. The amount of free space left depends on the size of
    the failed write.'''

    reported_full_date = pw.DateTimeField(null=True)
    '''The date/time this tape was last reported as being full.'''


class FileReference(DatabaseModel):
    '''A single file which could exist on multiple tapes'''

    name = pw.CharField(unique=True)
    '''A name for the file'''

    size = pw.BigIntegerField()
    '''The size of the file in bytes'''


class TapeIndex(DatabaseModel):
    '''Index for a single file on a single tape.'''

    tape = pw.ForeignKeyField(Tape, related_name='file_indices')
    '''The tape we are indexing into.'''

    file_ref = pw.ForeignKeyField(FileReference, related_name='tape_indices')
    '''The file we are indexing.'''

    block_idx = pw.BigIntegerField()
    '''The start block for the file on this tape'''

    file_idx = pw.BigIntegerField()
    '''The file index on this tape.'''

    pre_tape_info = pw.TextField()
    '''The result from `tapeinfo` just before the file was written.'''

    post_tape_info = pw.TextField()
    '''The result from `tapeinfo` just after the file was written.'''


class CleaningCartridge(DatabaseModel):
    '''A single cleaning cartridge'''

    barcode = pw.CharField(unique=True)
    '''Barcode that uniquely identifies the cartridge.'''

    is_expired = pw.BooleanField()
    '''Set to True if the cartridge has been used up.'''


class CleaningRun(DatabaseModel):
    '''A single cleaning run'''

    run_date = pw.DateTimeField()
    '''The date of the cleaning run'''

    cartridge = pw.ForeignKeyField(CleaningCartridge, related_name='runs')
    '''The cleaning cartridge'''


class NoSuitableCartridgeError(Exception):
    '''Raised when a cartridge suitable for the operation is not available in
    the changer'''


class UnknownTapeFormatError(Exception):
    '''Raised when the tape format cannot be determined'''


CLEANING_TIMEOUT_SECONDS = 5 * 60
'''Seconds to wait for a cleaning cycle to finish.'''


LTO_SIZES = {1 : 100 * 1000**3,
             2 : 200 * 1000**3,
             3 : 400 * 1000**3,
             4 : 800 * 1000**3,
             5 : 1500 * 1000**3, #TODO: reset this just testing
             6 : 2500 * 1000**3,
            }
'''Map LTO version number to the size of the media.'''


# TODO: Reset this just testing
TAPE_SIZE_SLOP = 2 * 1000**3
'''Assume the usable size of the tape is this much smaller than the
advertised size.'''


class MultiWritableError(Exception):
    '''Raised by a MultiWritable so the drive(s) that caused the error(s) can
    be identified'''
    def __init__(self, drives_and_errors):
        self.drives_and_errors = drives_and_errors

    def __str__(self):
        return '\n'.join('Exception from drive %s: %s' % drv_and_err
                         for drv_and_err in self.drives_and_errors)

class MultiWritable(object):
    '''Writable object that mirrors all writes one or more drives. Also keeps
    track of the number of bytes written.'''
    def __init__(self, drives):
        self.drives = drives
        self._bytes_written = 0
        self.files = []
        exceptions = []
        for drive in self.drives:
            try:
                self.files.append(drive.open('w'))
            except Exception, e:
                exceptions.append((drive, e))
        if len(exceptions) > 0:
            for f in self.files:
                f.close()
            raise MultiWritableError(exceptions)

    @property
    def bytes_written(self):
        return self._bytes_written

    def write(self, str):
        exceptions = []
        for idx, f in enumerate(self.files):
            try:
                f.write(str)
            except Exception, e:
                exceptions.append((self.drives[idx], e))
        if len(exceptions) > 0:
            raise MultiWritableError(exceptions)
        self._bytes_written += len(str)

    def flush(self):
        for f in self.files:
            f.flush()

    def close(self):
        for f in self.files:
            f.close()


class TapeWriter(object):
    '''Produced by the `TapeManager.generate_writers` method.

    Parameters
    ----------
    tape_mgr : TapeManager
        The TapeManager that created this writer.

    sets_and_drives : list
        A list of (set_name, drive) tuples specifying which tape_set is being
        written using which drive.
    '''
    def __init__(self, tape_mgr, sets_and_drives):
        self.tape_mgr = tape_mgr
        self.sets_and_drives = sets_and_drives

    @contextmanager
    def open(self, file_ref, size_is_approx=False):
        '''Context manager that produces a file-like object with at least the
        methods `write`, `flush`, and `close`. These commands are mirrored
        to one or more files on independent drives.

        The given 'file_ref' does not need to be in the database already. It
        will be saved into the database after a successful write.

        If `file_ref.size` is not known a priori you should provide the
        estimated maximum size and set `size_is_approx` to True.
        The size will automatically be updated to the number of bytes written
        after the operation is complete.

        It should be noted that an out-of-space error can occur even if less
        than `file_ref.size` bytes are written, due to errors (and thus
        reallocated blocks). The chances of this happening can be decreased
        by increasing the value of TAPE_SIZE_SLOP. If an out of space error
        occurs a tape will be added to any appropriate tape sets so that a
        future attempt at doing the write can succeed. Thus the user must
        allow any such exceptions to propogate out of the with block of the
        context manager.
        '''
        logger.debug("Writing file %s to sets and drives: %s", 
                     file_ref.name, 
                     self.sets_and_drives)
        # Prepare each tape set and drive combo
        set_info = {}
        drive_alerts = []
        for set_name, drive in self.sets_and_drives:
            # Make sure a tape with enough space is loaded
            curr_tape = self.tape_mgr._get_curr_tape(set_name)
            if curr_tape.est_free_space - file_ref.size < TAPE_SIZE_SLOP:
                curr_tape.is_full = True
                curr_tape.save()
                curr_tape = self.tape_mgr._add_new_tape(set_name)
            if drive.curr_tape != curr_tape.barcode:
                if drive.curr_tape is not None:
                    self.tape_mgr.changer.unload(drive)
                self.tape_mgr.changer.load(curr_tape.barcode)

            # Find the file and block index for the new file, position the
            # tape if needed
            block_idx = curr_tape.next_block
            try:
                last_idx = TapeIndex.select().\
                            where(TapeIndex.tape == curr_tape).\
                            order_by(TapeIndex.file_idx.desc()).\
                            get()
            except TapeIndex.DoesNotExist:
                new_file_idx = 0
            else:
                new_file_idx = last_idx.file_idx + 1
            if drive.tell_block() != block_idx:
                drive.seek_block(block_idx)

            # Get the pre-write tapeinfo and alerts
            try:
                pre_tape_info = drive.tape_info
            except UnhandledTapeAlertError, e:
                drive_alerts.append(e)
                pre_tape_info = e.tape_info

            # Keep track of some info we need for the database
            set_info[set_name] = [new_file_idx, pre_tape_info]

        # Handle any alerts
        if len(drive_alerts) > 0:
            self.tape_mgr._handle_alerts(drive_alerts)

        # Yield a file-like for writing, catch any out of space errors and
        # assign new tapes as needed
        try:
            f = MultiWritable([drive for _, drive in self.sets_and_drives])
            try:
                yield f
            finally:
                f.close()
        except MultiWritableError, e:
            for drive, exception in e.drives_and_errors:
                if hasattr(exception, 'errno') and exception.errno == errno.ENOSPC:
                    curr_tape.is_full = True
                    curr_tape.save()
                    self.tape_mgr._add_new_tape(curr_tape.tape_set.name)
            raise
        finally:
            # Get the post-write tape info and handle any alerts
            drive_alerts = []
            for set_name, drive in self.sets_and_drives:
                try:
                    post_tape_info = drive.tape_info
                except UnhandledTapeAlertError, e:
                    drive_alerts.append(e)
                    post_tape_info = e.tape_info
                set_info[set_name].append(post_tape_info)
            if len(drive_alerts) > 0:
                self.tape_mgr._handle_alerts(drive_alerts)

        # If the given file size was approximate, update it with the number
        # of bytes written
        if size_is_approx:
            file_ref.size = f.bytes_written

        # Add an index mapping the file to a tape in the database and update
        # the amount of free space on the tape
        for set_name, drive in self.sets_and_drives:
            # Pull out some basic info
            file_idx, pre_info, post_info = set_info[set_name]
            start_block = pre_info['Block Position']
            end_block = post_info['Block Position']
            curr_tape = Tape.get(Tape.barcode == drive.curr_tape)

            # If the block size is fixed we can determine the exact size on
            # tape, otherwise just use the amount of bytes specified in the
            # file_ref
            try:
                size_on_tape = (end_block - start_block) * drive.block_size
            except UnknownBlockSizeError:
                size_on_tape = file_ref.size

            # Update the database
            with self.tape_mgr.database.transaction():
                file_ref.save()
                tape_idx = TapeIndex.create(file_ref=file_ref,
                                            tape=curr_tape,
                                            block_idx=start_block,
                                            file_idx=file_idx,
                                            pre_tape_info=json.dumps(pre_tape_info),
                                            post_tape_info=json.dumps(post_tape_info)
                                           )
                curr_tape.est_free_space -= size_on_tape
                curr_tape.next_block = end_block
                curr_tape.save()


class TapeManager(object):
    '''Manages one or more tape sets for storage plus a tape set for cleaning.

    database : peewee.Database
        The database used for persistant storage

    changer : Changer
        The changer used for accessing tapes

    storage_choosers : dict
        Maps the name of each TapeSet to a callable that chooses the next
        tape. The callable takes a list of candidate tapes and returns the
        one to use next, or None if there are no viable candidates.

    cleaning_chooser : callable
        Takes a list of tapes and returns the cleaning cartridge to use next.

    init_db : bool
        If set to True the database will be initialized. Otherwise, if the
        database is not already initialized, an error will be raised.
    '''
    def __init__(self, database, changer, storage_choosers, cleaning_chooser,
                 init_db=False):
        self.database = database
        self.changer = changer
        self.storage_choosers = storage_choosers
        self.cleaning_chooser = cleaning_chooser

        if init_db:
            TapeSet.create_table()
            Tape.create_table()
            FileReference.create_table()
            TapeIndex.create_table()
            CleaningCartridge.create_table()
            CleaningRun.create_table()

            if len(storage_choosers) == 0:
                raise ValueError("Must provide at least one storage chooser")
            for set_name in storage_choosers.keys():
                TapeSet.create(name=set_name)
        else:
            if not all((TapeSet.table_exists(),
                        Tape.table_exists(),
                        FileReference.table_exists(),
                        TapeIndex.table_exists(),
                        CleaningCartridge.table_exists(),
                        CleaningRun.table_exists(),
                       )
                      ):
                raise UninitializedDatabaseError()

            #Add any tape sets that are not already in the database
            for set_name in storage_choosers.keys():
                db_set = TapeSet.select().where(TapeSet.name == set_name)
                if db_set.count() == 0:
                    TapeSet.create(name=set_name)

    def _get_unused(self):
        '''Get any available tapes (as barcodes) that are not already in the
        database as a Tape or a CleaningCartridge.'''
        unused = []
        for barcode in self.changer.tapes:
            if barcode == '':
                continue # Require a barcode
            try:
                Tape.get(Tape.barcode == barcode)
            except Tape.DoesNotExist:
                try:
                    CleaningCartridge.get(CleaningCartridge.barcode ==
                                          barcode)
                except CleaningCartridge.DoesNotExist:
                    unused.append(barcode)
        if len(unused) == 0:
            raise NoSuitableCartridgeError()
        return unused

    def _get_curr_tape(self, set_name):
        '''Get the current tape for the given set.

        Returns the tape with the most (estimated) free space.'''
        try:
            return Tape.select().join(TapeSet).\
                    where((Tape.is_full == False) & 
                          (TapeSet.name == set_name)).\
                    order_by(Tape.est_free_space.desc()).\
                    get()
        except Tape.DoesNotExist:
            return self._add_new_tape(set_name)

    def _add_new_tape(self, set_name):
        '''Adds an unused tape to the tape set and returns it.'''
        #T ODO: Can we check to make sure the EOD marker is at the beginning
        # of the tape? Probably need to allow the EOD to be at block zero
        # (for a blank tape) or block 1 (for a tape with a single EOF at the
        # beginning). In this case we need a drive to use...

        # Get a list of tapes that are not in the database (and have a
        # barcode)
        candidates = self._get_unused()

        # Let the chooser for the set determine which one to use
        new_barcode = self.storage_choosers[set_name](candidates)
        if new_barcode is None:
            raise NoSuitableCartridgeError()

        # Determine the LTO version from the barcode
        if new_barcode[-2] != 'L':
            raise UnknownTapeFormatError()
        try:
            lto_version = int(new_barcode[-1])
        except ValueError:
            raise UnknownTapeFormatError()

        # Add the tape to the database and return it
        tape_set = TapeSet.get(TapeSet.name == set_name)
        size = LTO_SIZES[lto_version]
        tape = Tape.create(barcode=new_barcode,
                           tape_set=tape_set,
                           size=size,
                           est_free_space=size,
                           next_block=0)
        logger.info("Added tape %s to set %s" % (new_barcode, set_name))
        return tape

    def _wait_for_clean(self):
        time.sleep(CLEANING_TIMEOUT_SECONDS)

    def _do_clean(self, drive):
        orig_tape = drive.curr_tape
        while True:
            try:
                # Try to grab the cleaning cartridge with the most uses that
                # is not expired
                cart = CleaningCartridge.select(CleaningCartridge,
                                                pw.fn.Count(CleaningRun.id)).\
                        join(CleaningRun, pw.JOIN_LEFT_OUTER).\
                        group_by(CleaningCartridge).\
                        where(CleaningCartridge.is_expired == False).\
                        order_by(pw.fn.Count(CleaningRun.id).desc()).get()
            except CleaningCartridge.DoesNotExist:
                # Otherwise select a new cleaning cartridge
                candidates = self._get_unused()
                new_barcode = self.cleaning_chooser(candidates)
                if new_barcode is None:
                    raise NoSuitableCartridgeError()
                cart = CleaningCartridge.create(barcode=new_barcode,
                                                is_expired=False)

            # Perform a cleaning run
            logger.info("Doing clean with cartridge %s" % cart.barcode)
            if drive.curr_tape is not None:
                self.changer.unload(drive)
            self.changer.load(cart.barcode, drive)
            self._wait_for_clean()

            try:
                tape_info = drive.tape_info
            except UnhandledTapeAlertError, e:
                # If there are tape alerts, see if the cleaning cartridge is
                # expired or there is some other alert
                unhandled = []
                for drive_info, alerts in e.unhandled:
                    drv_unhandled = []
                    for alert in alerts:
                        if alert.flag_num == EXPIRED_CLEAN_ALERT_FLAG:
                            logger.warn("The cleaning cartridge %s is expired" %
                                         cart.barcode)
                            cart.is_expired = True
                            cart.save()
                        elif (alert.flag_num == IS_CLEAN_CART_ALERT_FLAG or
                              alert.flag_num in CLEAN_ALERT_FLAGS):
                            continue
                        elif alert.flag_num in WARN_ALERT_FLAGS:
                            logger.warn("Non fatal TapeAlert: %s" % alert)
                        else:
                            drv_unhandled.append(alert)
                    if len(drv_unhandled) != 0:
                        unhandled.append((drive_info, drv_unhandled))

                # Raise an excpetion for unhandled alerts, break if the
                # cartridge was not marked as expired
                if len(unhandled) != 0:
                    raise UnhandledTapeAlertError(unhandled)
                elif not cart.is_expired:
                    break
            else:
                # If there are no tape alerts we are done
                break
            finally:
                self.changer.unload(drive)

        # Add the cleaning run to the database
        cleaning_run = CleaningRun.create(run_date=datetime.now(),
                                          cartridge=cart)

        # Put the original tape back in the drive
        if orig_tape is not None:
            self.changer.load(orig_tape, drive)

    def _handle_alerts(self, drive_exceptions):
        '''Handle any tape alerts from drives.
        '''
        # Go through the exceptions and look for cleaning requests, log
        # warnings for non-fatal alerts, and gather any fatal alerts
        unhandled = []
        cleaning_requested = set()
        for e in drive_exceptions:
            for (scsi_dev, barcode), alerts in e.unhandled:
                drv_unhandled = []
                for alert in alerts:
                    if alert.flag_num in CLEAN_ALERT_FLAGS:
                        cleaning_requested.add(scsi_dev)
                    elif alert.flag_num in WARN_ALERT_FLAGS:
                        logger.warn("Non fatal TapeAlert on device %r with "
                                    "tape %s: %s" % (scsi_dev,
                                                     barcode,
                                                     alert)
                                   )
                    else:
                        drv_unhandled.append(alert)
                if len(drv_unhandled) != 0:
                    unhandled.append(((scsi_dev, barcode), drv_unhandled))

        # Perform cleaning on any devices that requested it
        for scsi_dev in cleaning_requested:
            for drive in self.changer.drives:
                if drive.scsi_dev == scsi_dev:
                    self._do_clean(drive)

        # Raise an exception for any unhandled fatal alerts
        if len(unhandled) != 0:
            raise UnhandledTapeAlertError(unhandled)

    def generate_writers(self, set_names):
        '''Generates TapeWriter objects that can be used to dynamically write
        data to the given tape sets.

        The number of writers generated depends on the number of tape sets
        being written to and the number of available drives. All data must
        be (re)written to every writer that is generated in order to mirror
        the data to all the requested tape sets.
        '''
        # Figure out the current tape for each tape set, and which sets have
        # a tape already loaded in a drive
        curr_tapes = {}
        loaded_sets_and_drives = []
        unloaded_sets = []
        for set_name in set_names:
            curr_tape = self._get_curr_tape(set_name)
            curr_tapes[set_name] = curr_tape
            for drive in self.changer.drives:
                if drive.curr_tape == curr_tape.barcode:
                    loaded_sets_and_drives.append((set_name, drive))
                    break
            else:
                unloaded_sets.append(set_name)

        # Use any loaded drives immediately, load any unloaded sets as needed
        curr_drives = [x[1] for x in loaded_sets_and_drives]
        available_drives = [d for d in self.changer.drives
                            if d not in curr_drives]
        for set_name in unloaded_sets:
            # All the drives have been filled so we yield a TapeWriter
            if len(available_drives) == 0:
                yield TapeWriter(self, loaded_sets_and_drives)
                available_drives = [d for d in self.changer.drives]
                loaded_sets_and_drives = []

            # Load the current tape for the tape set into an available drive
            # and add the drive to our list of current drives
            drive = available_drives.pop(0)
            if drive.curr_tape is not None:
                self.changer.unload(drive)
            self.changer.load(curr_tapes[set_name].barcode, drive)
            loaded_sets_and_drives.append((set_name, drive))
        if len(loaded_sets_and_drives) != 0:
            yield TapeWriter(self, loaded_sets_and_drives)

    def write_files(self, set_names, src_files, src_refs=None,
                    block_size=256*1024):
        '''Write a collection of files to one or more tape sets.

        Parameters
        ----------
        set_names : tuple
            The names of the tape sets the files should be written to

        src_files : list
            The paths of the files we are writing.

        src_refs : list or None
            Provide the associated FileReference to use for each file.
            Otherwise the filename portion of the path is used the `name`
            attribute (and thus must be globally unique) and the size is
            found using a stat syscall.

        block_size : int
            The size of buffer to use for each read/write operation.
        '''
        for writer in self.generate_writers(set_names):
            for src_idx, src_file in enumerate(src_files):
                if src_refs is None:
                    file_size = os.stat(src_file).st_size
                    name = os.path.split(src_file)[1]
                    try:
                        file_ref = FileReference.get(FileReference.name == name)
                        assert file_ref.size == file_size
                    except FileReference.DoesNotExist:
                        file_ref = FileReference(name=name, size=file_size)
                else:
                    file_ref = src_refs[src_idx]
                
                with open(src_file, 'rb') as src_f:
                    try:
                        with writer.open(file_ref) as dest_f:
                            start = datetime.now()
                            while True:
                                buf = src_f.read(block_size)
                                dest_f.write(buf)
                                if len(buf) < block_size:
                                    break
                            end = datetime.now()
                    except MultiWritableError, e:
                        # If we got out of space errors try again as there may
                        # be another tape available to use
                        if all((hasattr(err, 'errno') and
                                err.errno == errno.ENOSPC)
                               for _, err in e.drives_and_errors):
                            with writer.open(file_ref) as dest_f:
                                start = datetime.now()
                                while True:
                                    buf = src_f.read(block_size)
                                    dest_f.write(buf)
                                    if len(buf) < block_size:
                                        break
                                end = datetime.now()
                        else:
                            raise
                bytes_per_sec = file_ref.size / total_seconds(end - start)
                logger.info('Wrote file %s (%d bytes) to tape at: %s', 
                            file_ref.name, 
                            file_ref.size, 
                            get_readable_bw(bytes_per_sec))

    @contextmanager
    def read_file(self, file_ref, tape_index=None):
        '''Context manager produces an open file (the one referenced by
        `file_ref`) for reading.

        If you want to read a specific copy of the file, the `tape_index` can
        be specified. Otherwise any available copy of the file will be used.
        Tapes that are already loaded in a drive will be preferred.
        '''
        drive = None
        if tape_index is None:
            # Find a tape in the changer that has the desired file, with a
            # preference for tapes that are already loaded in a drive.
            tapes = self.changer.tapes
            drive = None
            for t_idx in TapeIndex.select().where(TapeIndex.file_ref == file_ref):
                for d in self.changer.drives:
                    if d.curr_tape == t_idx.tape.barcode:
                        tape_index = t_idx
                        drive = d
                        break
                if drive is not None:
                    break

                if t_idx.tape.barcode in tapes:
                    tape_index = t_idx
            else:
                # We didn't find a loaded tape, make sure we found a tape
                if tape_index is None:
                    raise NoSuitableCartridgeError()
        else:
            # A specific instance of the file was requested, make sure the
            # request is valid and that we can access the tape
            if tape_index.file_ref != file_ref:
                raise ValueError("The given 'file_ref' and 'tape_index' "
                                 "don't match")
            if tape_index.tape.barcode not in self.changer.tapes:
                raise NoSuitableCartridgeError()
            # Check if the tape is already in a drive
            for d in self.changer.drives:
                if d.curr_tape == tape_index.tape.barcode:
                    drive = d
                    break

        # Choose a drive if we don't already have one
        if drive is None:
            # Use an empty drive if we can, otherwise use the first one
            for d in self.changer.drives:
                if d.curr_tape is None:
                    drive = d
                    break
            else:
                drive = self.changer.drives[0]
                self.changer.unload(drive)

        # Load the tape into the drive if it is not already
        self.changer.load(tape_index.tape.barcode, drive)

        # Check for tape alerts
        try:
            tape_info = drive.tape_info
        except UnhandledTapeAlertError, e:
            self._handle_alerts([e])

        # Position the tape at the beginning of the file
        drive.seek_file(tape_index.file_idx)

        # Open the file for reading and yield it
        try:
            f = drive.open('r')
            yield f
        finally:
            # Close the file and check for tape alerts
            f.close()
            try:
                tape_info = drive.tape_info
            except UnhandledTapeAlertError, e:
                self._handle_alerts([e])

