import os, string, logging, logging.handlers, smtplib
import subprocess as sp
from collections import namedtuple

import peewee as pw
from playhouse.sqlite_ext import SqliteExtDatabase


logger = logging.getLogger(__name__)


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
    logger.debug("Running command %s" % popen_args)
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


def get_free_space(target_path):
    '''Get the amount of free space (bytes) on the device the `target_path` 
    is on.
    '''
    if not os.path.exists(target_path):
        raise ValueError("The target path does not exist")
    stdout, _ = sp_exec(['df', '--block-size=1', target_path])
    lines = [x for x in stdout.split('\n') if x.strip() != '']
    toks = lines[-1].split()
    return int(toks[2])


class TapewormSqliteDatabase(SqliteExtDatabase):
    '''Simple wrapper for SqliteExtDatabase that enables needed features when 
    we connect.'''
    def connect(self, *args, **kwargs):
        super(TapewormSqliteDatabase, self).connect(*args, **kwargs)
        self.execute_sql('PRAGMA foreign_keys=ON;') 


database_proxy = pw.Proxy()


class DatabaseModel(pw.Model):
    class Meta:
        database = database_proxy

        
class UninitializedDatabaseError(Exception):
    '''Denotes that the database was not initialized.'''


def total_seconds(td):
    return ((td.microseconds + 
            ((td.seconds + (td.days * 24 * 3600)) * 10**6)) / 
           float(10**6))

def get_readable_bw(bytes_per_sec):
    '''Convert a floating point number giving bandwidth in bytes per second 
    to a human readable string'''
    val = bytes_per_sec
    for size in ('B', 'KB', 'MB', 'GB'):
        if val < 1024:
            return '%0.2f %s/sec' % (val, size)
        val /= 1024
    return '%0.2f TB/sec' % val


class BufferingSMTPHandler(logging.handlers.BufferingHandler):
    def __init__(self, mailhost, fromaddr, toaddrs, subject, capacity):
        logging.handlers.BufferingHandler.__init__(self, capacity)
        self.mailhost = mailhost
        self.mailport = None
        self.fromaddr = fromaddr
        self.toaddrs = toaddrs
        self.subject = subject
        
    def flush(self):
        if len(self.buffer) > 0:
            try:
                port = self.mailport
                if not port:
                    port = smtplib.SMTP_PORT
                smtp = smtplib.SMTP(self.mailhost, port)
                msg = ("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n" % 
                       (self.fromaddr, 
                        string.join(self.toaddrs, ","), 
                        self.subject)
                      )
                for record in self.buffer:
                    s = self.format(record)
                    msg = msg + s + "\r\n"
                smtp.sendmail(self.fromaddr, self.toaddrs, msg)
                smtp.quit()
            except:
                self.handleError(None)  # no particular record
            self.buffer = []

