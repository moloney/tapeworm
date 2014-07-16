import os, sys, time
import subprocess as sp
from glob import glob
from collections import namedtuple

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


watch_dir = sys.argv[1]
print "Watching directory %s" % watch_dir

while True:
    tar_gz_files = glob(os.path.join(watch_dir, '*.tar.gz'))
    for tar_gz_file in tar_gz_files:
        basename = '.'.join(tar_gz_file.split('.')[:-2])
        par2_file = basename + '.par2'
        par2_tar_file = par2_file + '.tar'
        if os.path.exists(par2_tar_file):
            print "Doing verification with file %s" % par2_file
            sp_exec(['tar', '-xvf', par2_tar_file, '-C', watch_dir])
            sp_exec(['par2', 'v', par2_file])
            print "Verification succeeded"
            cleanup_files = glob(basename + '*')
            for cleanup_file in cleanup_files:
                print "Cleaning up file %s" % cleanup_file
                os.remove(cleanup_file)
    time.sleep(5)
