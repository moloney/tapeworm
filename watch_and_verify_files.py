import os, sys, time, re
import subprocess as sp
from glob import glob
from collections import namedtuple
import pathmap

class NonZeroReturnException(Exception):
    def __init__(self, cmd, return_code, stdout, stderr):
        self.cmd = cmd
        self.return_code = return_code
        self.stdout = stdout
        self.stderr = stderr

    def __str__(self):
        return ('The command %s returned %d.\nCaptured stdout = %s\n'
                'Captured stderr = %s' % 
                (self.cmd, self.return_code, self.stdout, self.stderr)
               )


Process = namedtuple('Process', 'proc popen_args')
'''Store the Popen object and the arguments used to create it'''

               
def sp_start(popen_args, shell=False):
    proc = sp.Popen(popen_args, 
                    stdin=sp.PIPE, 
                    stdout=sp.PIPE,
                    stderr=sp.PIPE, 
                    shell=shell)
    return Process(proc, popen_args)
    

def sp_finish(process, stdin=None):
    output = process.proc.communicate(stdin)
    if process.proc.returncode != 0:
        raise NonZeroReturnException(process.popen_args, 
                                     process.proc.returncode,
                                     output[0], 
                                     output[1])
    return output


def sp_exec(popen_args, stdin=None, shell=False):
    '''Convienance wrapper for subprocess.Popen.

    Parameters
    ----------
    popen_args : list or str
        The command to execute and any arguments to pass to it

    stdin : str or None
        Pipe in the given string to the subprocess through stdin

    shell : bool
        If set to True, the command will be run through a shell.

    Returns
    -------
    stdout : str
        The resulting output from stdout

    stderr : str
        The resulting output from stderr

    Raises
    ------
    e : NonZeroReturnException
        Raised if the return code is not zero.
    '''
    process = sp_start(popen_args, shell=shell)
    return sp_finish(process, stdin)


watch_dir, src_root = sys.argv[1:]
print "Watching directory %s and comparing to src dir %s" % (watch_dir, src_root)
watch_dir = os.path.abspath(watch_dir)
pm = pathmap.PathMap(re.escape(watch_dir) + '/(.+)', ignore_rules=[lambda p,d: d.is_dir()])
n_cmp = 100*1024**2 # Compare first/last 100MB
n_cmp_str = str(n_cmp)

while True:
    for match_result in pm.matches(watch_dir):
        src_path = os.path.join(src_root, match_result.match_info[1])
        restore_path = match_result.path
        src_size = os.stat(src_path).st_size
        restore_size = os.stat(restore_path).st_size
        # If the sizes don't match, assume the restore isn't finished
        if restore_size != src_size:
            print "Waiting for file to complete: %s" % restore_path
            continue
        # Compare the files, deleting the restored one if successful
        print "Comparing files %s and %s" % (src_path, restore_path)
        sp_exec(['cmp', '-n', n_cmp_str, src_path, restore_path])
        if src_size > n_cmp:
            sp_exec(['cmp', '-i', str(src_size - n_cmp), src_path, restore_path])
        print "Comparison successful, cleaning up %s" % restore_path
        os.remove(restore_path)
        
    time.sleep(5)
