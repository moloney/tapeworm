import os, logging
from os import path
from glob import glob
from datetime import datetime
from contextlib import contextmanager
try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

from util import get_readable_bw, total_seconds

logger = logging.getLogger(__name__)


class OutOfSpoolError(Exception):
    '''Indicates there is not enough space on a spool device, even after 
    flushing all completed writes.'''


READ_DIR = 'TAPEWORM_SPOOL_READ'
'''Prefix used to create the spooled file name for reads.'''


WRITE_DIR = 'TAPEWORM_SPOOL_WRITE'
'''Prefix used to create the spooled file name for writes.'''


class Spool(object):
    '''Specifies a spool directory where files are created before being 
    written to tape. Allows you to avoid "shoe shining" when a data source 
    is not fast enough to feed the tape drive by using a faster spool device
    as an intermediary.
    '''
    def __init__(self, tape_mgr, dir_path, size_limit, block_size=256*1024):
        self.tape_mgr = tape_mgr
        self.dir_path = dir_path
        self.size_limit = size_limit
        self.block_size = block_size
        self._read_dir = path.join(self.dir_path, READ_DIR)
        self._write_dir = path.join(self.dir_path, WRITE_DIR)
        self._is_init = False
        self._pre_cb = None
        self._post_cb = None
        self._max_open_write_size = 0
        self._finalized_writes = OrderedDict()
        self._finalized_write_size = 0
        self._open_read_size = 0

    def init(self):
        '''Setup the spool directory. Must be called before using the spool.
        
        Will clear out any existing spool files, so this should only be 
        called if no other process is using the same spool directory.'''
        if self._is_init:
            raise ValueError("The spool was already initialized")
        for sub_dir in (self._read_dir, self._write_dir):
            if not path.exists(sub_dir):
                os.mkdir(sub_dir)
            else:
                stale = glob(path.join(sub_dir, '*'))
                for file_path in stale:
                    logger.warn("Removing stale spool file %s", file_path)
                    os.remove(file_path)
        self._is_init = True

    @property
    def is_init(self):
        return self._is_init

    @property
    def min_free_space(self):
        return (self.size_limit - 
                self._max_open_write_size - 
                self._finalized_write_size - 
                self._open_read_size)
    
    @property
    def max_free_space(self):
        return (self.size_limit - 
                self._max_open_write_size - 
                self._open_read_size)
                
    def _check_spool(self, file_ref):
        if file_ref.size > self.size_limit:
            raise ValueError("The file size exceeds the entire size limit of "
                             "the spool")
        if file_ref.size > self.min_free_space:
            if file_ref.size > self.max_free_space:
                raise OutOfSpoolError()
            self.flush()
            
    def set_flush_callbacks(self, pre_flush_cb=None, post_flush_cb=None):
        '''Set functions to be called before and after each time the spool is 
        flushed. The `post_flush_cb` be called even if an exception occurred, 
        so it is up to the callback to figure out which files were written 
        successfully (i.e. by interrogating the database).
        '''
        self._pre_cb = pre_flush_cb
        self._post_cb = post_flush_cb
    
    def get_spooled_path(self, file_ref, read=False):
        if read:
            return path.join(self._read_dir, file_ref.name)
        else:
            return path.join(self._write_dir, file_ref.name)
    
    @contextmanager
    def write(self, set_names, file_ref, size_is_approx=False):
        '''Context manager produces a file-like for writing.'''
        if not self._is_init:
            raise ValueError("The spool has not been initialized")
            
        self._check_spool(file_ref)
        set_names = tuple(set_names)
            
        self._max_open_write_size += file_ref.size
        file_path = self.get_spooled_path(file_ref)
        f = open(file_path, 'w')
        try:
            yield f
        except BaseException:
            f.close()
            os.remove(file_path)
            self._max_open_write_size -= file_ref.size
            raise
        else:
            f.close()
            self._max_open_write_size -= file_ref.size
            if size_is_approx:
                file_ref.size = os.stat(file_path).st_size
            self._finalized_write_size += file_ref.size
            if not set_names in self._finalized_writes:
                self._finalized_writes[set_names] = ([], [])
            write_info = self._finalized_writes[set_names]
            write_info[0].append(file_path)
            write_info[1].append(file_ref)

    @contextmanager
    def read(self, file_ref, tape_idx=None):
        '''Context manager produces a file-like for reading.'''
        if not self._is_init:
            raise ValueError("The spool has not been initialized")
            
        self._check_spool(file_ref)
    
        self._open_read_size += file_ref.size
        file_path = self.get_spooled_path(file_ref, read=True)
        
        # Read the file from tape and write to the spool file
        try:
            with self.tape_mgr.read_file(file_ref, tape_idx) as src_f:
                with open(file_path, 'w') as dest_f:
                    start = datetime.now()
                    while True:
                        buf = src_f.read(self.block_size)
                        dest_f.write(buf)
                        if len(buf) < self.block_size:
                            break
                    end = datetime.now()
        except BaseException:
            os.remove(file_path)
            self._open_read_size -= file_ref.size
            raise
        bytes_per_sec = file_ref.size / total_seconds(end - start)
        logger.info('Read file %s (%d bytes) from tape at: %s', 
                    file_ref.name, 
                    file_ref.size, 
                    get_readable_bw(bytes_per_sec))

        # Open the spool file for reading and yield it
        f = open(file_path, 'r')
        try:
            yield f
        finally:
            f.close()
            os.remove(file_path)
            self._open_read_size -= file_ref.size
    
    def flush(self):
        '''Flush any pending writes to tape'''
        if not self._is_init:
            raise ValueError("The spool has not been initialized")
        
        n_files = len(self._finalized_writes)
        if n_files == 0:
            return
            
        logger.debug("Starting to flush files from spool to tapes")
        if self._pre_cb is not None:
            self._pre_cb()
        try:
            for set_names, (paths, refs) in self._finalized_writes.iteritems():
                self.tape_mgr.write_files(set_names, 
                                          paths, 
                                          refs, 
                                          block_size=self.block_size)
                for file_idx, file_path in enumerate(paths):
                    file_ref = refs[file_idx]
                    self._finalized_write_size -= file_ref.size
                    os.remove(file_path)
            self._finalized_writes.clear()
        finally:
            if self._post_cb is not None:
                self._post_cb()
        logger.debug("All files were flushed successfully")
    
    
