import os, sys, shutil, types
import nose
from os import path
from tempfile import mkdtemp
import peewee as pw
from playhouse.sqlite_ext import SqliteExtDatabase
from nose.tools import eq_, ok_
from nose.exc import SkipTest

# Make sure we test the local source code rather than the installed copy
test_dir = os.path.dirname(__file__)
src_dir = os.path.normpath(os.path.join(test_dir, '..'))
sys.path.insert(0, src_dir)
from tapeworm.util import database_proxy, sp_exec
from tapeworm import tapemgr
from test_devctrl import get_changer

# We need to monkey patch some constants to align with our VTL setup for
# testing (5MB size per tape)
tapemgr.TAPE_SIZE_SLOP = 0
tapemgr.LTO_SIZES[5] = 5 * 1000**2
tapemgr.LTO_SIZES[6] = 5 * 1000**2


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
                    'offsite_0' : storage_chooser,
                    'offsite_1' : storage_chooser,
                   }


def make_expired_wait_for_clean(drive_id, tape_mgr):
    '''Create a function that can monkey patch TapeManager._wait_for_clean in
    order to generate a TapeAlert indicating that the cleaning cartridge is
    expired.
    '''
    tape_mgr._alert_raised = False
    def _wait_for_clean(self):
        if not self._alert_raised:
            sp_exec(['vtlcmd', drive_id, 'TapeAlert', '200000'])
            self._alert_raised = True
    return types.MethodType(_wait_for_clean, tape_mgr)


def get_tape_mgr(db=None):
    changer = get_changer()
    if db is None:
        db = SqliteExtDatabase(':memory:')
        db.connect()
        database_proxy.initialize(db)
    return tapemgr.TapeManager(db,
                               changer,
                               storage_choosers,
                               cleaning_chooser,
                               init_db=True)

class TestTapeManager(object):
    def setup(self):
        self.temp_dir = mkdtemp(prefix='tapeworm_tapemgr_test')
        self.tape_mgr = get_tape_mgr()

    def teardown(self):
        database_proxy.close()
        shutil.rmtree(self.temp_dir)

    def test_write_and_read(self):
        # Write some data to all of the tapesets
        for file_idx in xrange(3):
            data = 'This is test run %d' % file_idx
            file_name = 'test_%d' % file_idx
            file_ref = tapemgr.FileReference(name=file_name, size=len(data))
            n_sets = len(storage_choosers)
            n_drives = len(self.tape_mgr.changer.drives)
            expected_loops = n_sets / n_drives
            if n_sets % n_drives != 0:
                expected_loops += 1
            n_loops = 0
            for writer in self.tape_mgr.generate_writers(storage_choosers.keys()):
                with writer.open(file_ref) as f:
                    f.write(data)
                n_loops += 1
            eq_(n_loops, expected_loops)

            # Check that the database was updated correctly
            t_indices = [x for x in tapemgr.TapeIndex.\
                            select().\
                            where(tapemgr.TapeIndex.file_ref == file_ref)
                        ]
            eq_(len(t_indices), n_sets)
            tapes = [x.tape for x in t_indices]
            barcodes = [x.barcode for x in tapes]
            eq_(len(set(barcodes)), n_sets)
            indexed_sets = [x.tape_set.name for x in tapes]
            eq_(set(indexed_sets), set(storage_choosers.keys()))

            # Read each copy of the file and make sure the contents are correct
            for t_idx in t_indices:
                with self.tape_mgr.read_file(t_idx.file_ref, t_idx) as f:
                    res = f.read(t_idx.file_ref.size)
                eq_(res, data)

    def test_write_files(self):
        # Create the files in the temp dir
        file_paths = []
        for file_idx in xrange(3):
            data = 'This is test run %d' % file_idx
            file_name = 'test_%d' % file_idx
            file_path = path.join(self.temp_dir, file_name)
            file_paths.append(file_path)
            with open(file_path, 'w') as f:
                f.write(data)

        # Write the files to tapes
        self.tape_mgr.write_files(storage_choosers.keys(), file_paths)

        # Read back and verify the results
        for file_idx, file_path in enumerate(file_paths):
            data = 'This is test run %d' % file_idx
            file_name = 'test_%d' % file_idx
            file_ref = tapemgr.FileReference.get(tapemgr.FileReference.name == file_name)

            # Make sure it works without a tape index
            with self.tape_mgr.read_file(file_ref) as f:
                res = f.read()
            eq_(res, data)

            # Make sure it works with a tape index
            for t_idx in tapemgr.TapeIndex.select().\
                            where(tapemgr.TapeIndex.file_ref == file_ref):
                with self.tape_mgr.read_file(t_idx.file_ref, t_idx) as f:
                    res = f.read()
                eq_(res, data)

    def test_clean(self):
        tapemgr.CLEANING_TIMEOUT_SECONDS = 1
        data = 'This is a test'
        file_name = 'test0'
        file_ref = tapemgr.FileReference(name=file_name, size=len(data))
        set_names = [storage_choosers.keys()[0]]
        n_cleaning_runs = 0
        for writer in self.tape_mgr.generate_writers(set_names):
            with writer.open(file_ref) as f:
                f.write(data)
                # Generate a "Clean now" tape alert on the first drive
                # The '11' is a reference to the drive number assigned in the
                # mhVTL config file 'device.conf'
                sp_exec(['vtlcmd', '11', 'TapeAlert', '80000'])
                n_cleaning_runs += 1
        clean_runs = [x for x in tapemgr.CleaningRun.select()]
        eq_(len(clean_runs), n_cleaning_runs)

        with self.tape_mgr.read_file(file_ref) as f:
            res = f.read(file_ref.size)
            sp_exec(['vtlcmd', '11', 'TapeAlert', '80000'])
            n_cleaning_runs += 1
        eq_(res, data)
        clean_runs = [x for x in tapemgr.CleaningRun.select()]
        eq_(len(clean_runs), n_cleaning_runs)
        clean_carts = [x for x in tapemgr.CleaningCartridge.select()]
        eq_(len(clean_carts), 1)

    def test_expired_clean(self):
        self.tape_mgr._wait_for_clean = make_expired_wait_for_clean('11', self.tape_mgr)
        data = 'This is a test'
        file_name = 'test0'
        file_ref = tapemgr.FileReference(name=file_name, size=len(data))
        set_names = [storage_choosers.keys()[0]]
        n_cleaning_runs = 0
        for writer in self.tape_mgr.generate_writers(set_names):
            with writer.open(file_ref) as f:
                f.write(data)
                sp_exec(['vtlcmd', '11', 'TapeAlert', '80000'])
                n_cleaning_runs += 1
        clean_runs = [x for x in tapemgr.CleaningRun.select()]
        eq_(len(clean_runs), n_cleaning_runs)
        clean_carts = [x for x in tapemgr.CleaningCartridge.select()]
        eq_(len(clean_carts), 2)
        expired_carts = [x for x in clean_carts if x.is_expired]
        eq_(len(expired_carts), 1)
