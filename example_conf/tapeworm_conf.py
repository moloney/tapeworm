import logging
import peewee as pw
from pathmap import PathMap
from tapeworm.util import TapewormSqliteDatabase, BufferingSMTPHandler
from tapeworm.devctrl import get_changers
from tapeworm.jobs import BackupJobSpec


# Configure logging
LOG_FORMAT = '%(asctime)s %(levelname)s %(name)s %(message)s'
formatter = logging.Formatter(LOG_FORMAT)
tw_logger = logging.getLogger('tapeworm')
tw_logger.setLevel(logging.DEBUG)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.INFO)
tw_logger.addHandler(stream_handler)

file_handler = logging.FileHandler('./tapeworm.log')
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)
tw_logger.addHandler(file_handler)

MAILHOST = 'mysmtpserver'
FROM     = 'noreply@myhost'
TO       = ['myemail@mydomain.com']
SUBJECT  = 'Tapeworm Information'
mail_handler = BufferingSMTPHandler(MAILHOST, FROM, TO, SUBJECT, 50)
mail_handler.setFormatter(formatter)
mail_handler.setLevel(logging.WARN)
tw_logger.addHandler(mail_handler)

# Any handlers here will be flushed after running the CLI or processing the 
# work queue
buffered_handlers = [mail_handler]


# Define functions that handle selection of new tapes/cartridges
cleaning_prefix = 'CLN'


def cleaning_chooser(barcodes):
    '''Choose cleaning tapes based on prefix'''
    for barcode in barcodes:
        if barcode.startswith(cleaning_prefix):
            return barcode


def storage_chooser(barcodes):
    '''Choose storage tapes by avoiding cleaning tapes and prefering LTO5'''
    selection = None
    for barcode in barcodes:
        if barcode.startswith(cleaning_prefix):
            continue
        if barcode.endswith('L5'):
            return barcode
        elif selection is None:
            selection = barcode
    return selection
    

# Define onsite and offsite storage tape sets, both of which prefer LTO-5
storage_choosers = {'onsite' : storage_chooser,
                    'offsite' : storage_chooser,
                   }


# Assume we have one tape changer
changers = get_changers()
assert len(changers) == 1
changer = changers[0]


# Set up the database
# Currently we rely on the locking features provided by the SqliteExtDatabase 
# so this is the only valid option here. This database file must also reside 
# on a filesystem that supports file locking (i.e. not NFS).
# We also increase the timeout value since I have seem timeout errors even 
# with relatively light contention. This may be due to sqlite being compiled 
# without HAVE_USLEEP defined 
# (see: http://beets.radbox.org/blog/sqlite-nightmare.html)
db = TapewormSqliteDatabase('/path/to/tapeworm.db', timeout=120)
db.connect()


# Specify where index files should be stored. The files created here will be 
# referenced by the database and thus should not be altered outside of this 
# software. The contents can be moved in there entirety however (when the 
# software is not running) and then have the location updated here. 
index_dir = '/path/to/indices'


# Set the spool path and size.  This device should be fast enough to feed 
# all drives at their minimum streaming speed.
spool_path = '/path/to/spool'
spool_size = 360*1024**3


# Set the block size to use when writing to or reading from tape. If the 
# block size on the drive is fixed (generally this should not be the case) 
# this needs to be an even multiple of that number. If the block size on the 
# drive is dynamic, then any reads just need to be done with the same block 
# size as the writes. For example, if you wanted to bypass tapeworm all 
# together an just use 'dd' to grab a file from tape, you must specify the 
# correct block size.
block_size = 256*1024


# Define some parameters used by the JobManager
max_archive_size = 80*1024**3


# Define a backup job specifications and the root directories for any backup 
# runs we are about to start. Changes to the BackupJobSpec's will require 
# any running daemon processes to be restarted for it to take effect.
job_specs = []
root_dirs = {}
pm = PathMap('.+/(.+)', ignore_rules=['lost\+found'], depth=1)
for match in pm.matches('/path/to/backup'):
    job_name = match.match_info[1]
    job_specs.append(BackupJobSpec(job_name, 
                                   storage_choosers.keys(),
                                   par_percent=5,
                                  )
                    )
    root_dirs[job_name] = match.path
                                
                                  


