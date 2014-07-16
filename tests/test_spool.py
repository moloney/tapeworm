import os, sys, shutil, types
import nose
from os import path
from tempfile import mkdtemp
import peewee as pw
from nose.tools import *
from nose.exc import SkipTest


# Make sure we test the local source code rather than the installed copy
test_dir = os.path.dirname(__file__)
src_dir = os.path.normpath(os.path.join(test_dir, '..'))
sys.path.insert(0, src_dir)
from tapeworm.util import database_proxy, sp_exec
from tapeworm.tapemgr import FileReference
from tapeworm import tapemgr, spool
from test_tapemgr import get_tape_mgr, storage_choosers


class TestSpool(object):
    def setup(self):
        self.temp_dir = mkdtemp(prefix='tapeworm_spool_test')
        self.tape_mgr = get_tape_mgr()
        self.spool_size = 1024
        self.spool = spool.Spool(self.tape_mgr, 
                                 self.temp_dir, 
                                 self.spool_size)
        self.spool.init()

    def teardown(self):
        database_proxy.close()
        shutil.rmtree(self.temp_dir)
        
    def test_write_and_read(self):
        # Fill the spool with writes
        data_fmt = 'This is test data for test_%04d'
        data_size = len(data_fmt % 0)
        max_spooled = self.spool_size  / data_size
        dest_sets = storage_choosers.keys()
        for idx in xrange(max_spooled):
            data = data_fmt % idx
            file_ref = FileReference(name=('test_%d' % idx), size=data_size)
            with self.spool.write(dest_sets, file_ref) as dest_f:
                dest_f.write(data)
        
        # Nothing should have made it to tape yet
        eq_(len([x for x in FileReference.select()]), 0)
        
        # Writing one more file will force a flush of the previously spooled
        idx += 1
        data = data_fmt % idx
        file_ref = FileReference(name=('test_%d' % idx), size=data_size)
        with self.spool.write(dest_sets, file_ref) as dest_f:
            dest_f.write(data)
        eq_(len([x for x in FileReference.select()]), max_spooled)
            
        # Fill the spool again
        for idx in xrange(max_spooled + 1, max_spooled * 2):
            data = data_fmt % idx
            file_ref = FileReference(name=('test_%d' % idx), size=data_size)
            with self.spool.write(dest_sets, file_ref) as dest_f:
                dest_f.write(data)
        eq_(len([x for x in FileReference.select()]), max_spooled)
    
        # Reading a file will force a flush
        idx = 0
        data = data_fmt % idx
        file_ref = FileReference.get(FileReference.name == 'test_%d' % idx)
        with self.spool.read(file_ref) as src_f:
            result = src_f.read()
        eq_(result, data)
        eq_(len([x for x in FileReference.select()]), max_spooled * 2)
        
        # The spooled read is cleaned up immediately after the with block
        eq_(self.spool.min_free_space, self.spool.max_free_space)
        
