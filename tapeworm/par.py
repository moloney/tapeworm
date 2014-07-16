import os, logging
from collections import deque
from tempfile import mkdtemp
from glob import glob

from util import sp_start, sp_finish


logger = logging.getLogger(__name__)


class ParFactory(object):
    def __init__(self, base_dir=None):
        self._tmp_dir = mkdtemp(prefix='twpar2-temp', dir=base_dir)

        self._wait_deque = deque()
        self._done_deque = deque()
        self._curr_item = None
        self._curr_proc = None

    def _start_proc(self, src_path, hdr_path, percent_redundancy):
        logger.info("Creating par2 for file %s", src_path)
        popen_args = ['par2',
                      'create',
                      '-r%02d' % percent_redundancy,
                      '-n1',
                      hdr_path,
                      src_path]
        return sp_start(popen_args)

    def update(self):
        '''Check if any items are completed, starting a new process if needed.
        '''
        # Check if the current process is done
        if (self._curr_item is not None and
            self._curr_proc.proc.poll() is not None
           ):
            sp_finish(self._curr_proc)
            # We have a header plus a data file which we use glob to find,  
            # make sure the header files comes first
            path_toks = self._curr_item[1].split('.')
            hdr_basename = '.'.join(path_toks[:-1])
            hdr_name = hdr_basename + '.par2'
            results_paths = [hdr_name]
            results_paths.extend(x for x in glob(hdr_basename + '*.par2')
                                 if x != hdr_name)
            self._done_deque.append(results_paths)
            self._curr_item = None

        # Check if we need to start a new process
        if self._curr_item is None and len(self._wait_deque) != 0:
            self._curr_item = self._wait_deque.popleft()
            self._curr_proc = self._start_proc(*self._curr_item)

    def queue(self, src_path, dest_basename, percent_redundancy):
        '''Queue the creation of a par2 archive for the given file. Returns 
        the path where the par2 header file will be created.'''
        hdr_path = os.path.join(self._tmp_dir, dest_basename + '.par2')
        self._wait_deque.append((src_path, 
                                 hdr_path, 
                                 percent_redundancy))
        self.update()
        return hdr_path

    @property
    def n_queued(self):
        '''Return the number of queued jobs.'''
        self.update()
        result = len(self._wait_deque)
        if self._curr_item is not None:
            result += 1
        return result

    @property
    def n_done(self):
        '''The number of jobs that are done.'''
        self.update()
        return len(self._done_deque)

    def get_completed(self):
        '''Return a list of lists of paths for finished par2 archives.'''
        # If we have completed items return them, otherwise wait for one
        self.update()
        result = list(self._done_deque)
        self._done_deque.clear()
        return result
