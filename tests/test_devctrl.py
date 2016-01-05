import os, sys
import nose
from contextlib import closing
from nose.tools import eq_, ok_
from nose.exc import SkipTest


# Make sure we test the local source code rather than the installed copy
test_dir = os.path.dirname(__file__)
src_dir = os.path.normpath(os.path.join(test_dir, '..'))
sys.path.insert(0, src_dir)
from tapeworm import lsscsi, devctrl, tapeinfo


# Make sure we have a properly configured virtual tape library
scsi_devs = lsscsi.get_scsi_devs()
if any(d.scsi_model.manufacturer == 'TWTST' for d in scsi_devs):
    has_test_changer = True
else:
    has_test_changer = False


def requires_vtl(f):
    '''Decorator for any tests that require VTL changer device for testing'''
    def wrapper(*args, **kwargs):
        if not has_test_changer:
            raise SkipTest("Can't find properly configured virtual tape "
                           "library")
        return f(*args, **kwargs)
    return wrapper


@requires_vtl
def get_changer():
    for ch in devctrl.get_changers('tape'):
        if (ch.scsi_dev.scsi_model.manufacturer == 'TWTST' and
            ch.scsi_dev.scsi_model.model == 'TWTST'
           ):
            # Don't need this delay when testing on a VTL and it slows down the
            # tests significantly
            ch.load_delay = 0

            # Make sure all drives are unloaded to start with and clear out any
            # pre-existing TapeAlerts so they don't interfere with our tests
            # (e.g. restarting mhvtl while a drive is still "loaded" will
            # generate a TapeAlert)
            for drive in ch.drives:
                tapeinfo.get_tape_info(drive)
                if drive.curr_tape is not None:
                    ch.unload(drive)

            return ch


def test_get_changer():
    ch = get_changer()

    ok_(ch is not None)


class TestChangerAndDrives(object):
    def setup(self):
        self.ch = get_changer()

    def test_load_unload(self):
        # Load each drive
        slots = []
        for drive in self.ch.drives:
            for slot in self.ch.slots:
                if slot.curr_tape != None:
                    tape = slot.curr_tape
                    slots.append(slot)
                    break

            ok_('DR_OPEN' in drive.flags)
            eq_(drive, self.ch.load(tape, drive))
            ok_('ONLINE' in drive.flags)
            eq_(drive.curr_tape, tape)
            eq_(slots[-1].curr_tape, None)

        # Unload each drive
        for idx, drive in enumerate(self.ch.drives):
            tape = drive.curr_tape
            self.ch.unload(drive, slots[idx])
            eq_(drive.curr_tape, None)
            eq_(slots[idx].curr_tape, tape)

    def test_write_and_read(self):
        drive = self.ch.load(self.ch.tapes[0])
        eq_(drive.tape_info['BlockSize'], 0) # Require dynamic block size
        eq_(drive.tell_block(), 0)
        with drive.open('w') as f:
            f.write("This is a test...")
        with drive.open('w') as f:
            f.write("This is another test...")
        last_block = drive.tell_block()
        ok_(last_block != 0)
        drive.rewind()
        eq_(drive.tell_block(), 0)
        with drive.open('r') as f:
            eq_(f.read(), "This is a test...")
        with drive.open('r') as f:
            eq_(f.read(), "This is another test...")
        eq_(drive.tell_block(), last_block)
