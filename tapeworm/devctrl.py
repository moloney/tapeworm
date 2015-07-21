import re, functools, time
from copy import deepcopy

from util import sp_exec
from lsscsi import get_scsi_devs
from mtx import mtx_command
from tapeinfo import get_tape_info, UnhandledTapeAlertError


class NoMediaError(Exception):
    '''Indicates an error due to there being no media in the drive.'''
    
    
class UnknownBlockSizeError(Exception):
    '''Indicates an error due to the block size being dynamic'''


def _dirties_status(wrapped):
    def wrapper(self, *args, **kwargs):
        try:
            res = wrapped(self, *args, **kwargs)
        finally:
            self._status_dirty = True
        return res
    return wrapper


def _needs_status(wrapped=None, needs_media=False):
    if wrapped is None:
        return functools.partial(_needs_status, needs_media=needs_media)
    @functools.wraps(wrapped)
    def wrapper(self, *args, **kwargs):
        if self._status_dirty:
            self._update_status()
        if needs_media and 'DR_OPEN' in self._flags:
            raise NoMediaError()
        return wrapped(self, *args, **kwargs)
    return wrapper


class Drive(object):
    def __init__(self, scsi_dev, curr_tape=None):
        '''Control the given tape drive.'''
        self.scsi_dev = scsi_dev
        self._curr_tape = curr_tape

        #Determine the non-rewinding tape device
        match = re.match('/dev/st([0-9]+)', scsi_dev.spec_dev)
        dev_num = int(match.groups()[0])
        self.nr_spec_dev = '/dev/nst%d' % dev_num
        
        self._status_dirty = True

    def __repr__(self):
        return ('Drive(scsi_dev=%r, curr_tape=%r)' %
                (self.scsi_dev, self.curr_tape))
                
    def _update_status(self):
        # Get any set flags from the status bits from 'mt status'
        stdout, _ = sp_exec(['mt', '-f', self.nr_spec_dev, 'status'])
        found_flags = False
        for line in stdout.split('\n'):
            line = line.strip()
            if line.startswith('General status bits'):
                found_flags = True
            elif found_flags:
                self._flags = line.split()
                found_flags = False
                
        self._status_dirty = False
        
        # Make sure the drive status matches our curr_tape status
        if 'DR_OPEN' in self._flags:
            assert self._curr_tape is None
        else:
            assert self._curr_tape is not None
            
        # Update our tape info, raise an exception for any tape alerts
        self._tape_info, alerts = get_tape_info(self)
        if len(alerts) > 0:
            drive_info = (self.scsi_dev, self._curr_tape)
            raise UnhandledTapeAlertError([(drive_info, alerts)], 
                                          self._tape_info)
    
    @property
    def curr_tape(self):
        return self._curr_tape
        
    @curr_tape.setter
    @_dirties_status
    def curr_tape(self, val):
        self._curr_tape = val
        
    @property
    @_needs_status()
    def tape_info(self):
        return deepcopy(self._tape_info)
    
    @property
    @_needs_status(needs_media=True)
    def block_size(self):
        '''The current block size.
        
        Raises a UnknownBlockSizeError if the block size is not a fixed size.
        '''
        bs = self._tape_info['BlockSize'] 
        if bs == 0:
            raise UnknownBlockSizeError()
        return bs
        
    @property
    @_needs_status
    def flags(self):
        return tuple(self._flags)
    
    @_needs_status(needs_media=True)
    def tell_block(self):
        '''Return the current block position'''
        return self._tape_info['Block Position']
        
    def tell_byte(self):
        '''Tell the current byte position.
        
        Raises a UnknownBlockSizeError if the block size is not a fixed size.
        '''
        return self.tell_block() * self.block_size
        
    @_dirties_status
    def seek_block(self, block_num):
        '''Seek to the given block'''
        sp_exec(['mt', '-f', self.nr_spec_dev, 'seek', str(block_num)])

    @_dirties_status
    def seek_file(self, file_num):
        '''Seek to the first block of the given file'''
        sp_exec(['mt', '-f', self.nr_spec_dev, 'asf', str(file_num)])

    @_dirties_status
    def seek_eod(self):
        '''Seek to the end of the data on the tape'''
        sp_exec(['mt', '-f', self.nr_spec_dev, 'seod'])
    
    @_dirties_status
    def rewind(self):
        '''Rewind to beginning of the tape'''
        sp_exec(['mt', '-f', self.nr_spec_dev, 'rewind'])
        
    @_dirties_status
    def open(self, flag='r'):
        '''Open a file on the current tape, at the current location, for 
        reading or writing.'''
        return open(self.nr_spec_dev, flag)
    
    def mark_dirty(self):
        self._status_dirty = True


class Slot(object):
    '''Represents a slot in a media changer'''
    def __init__(self, curr_tape=None):
        self.curr_tape = curr_tape


class UnknownTapeError(Exception):
    '''Indicates the tape is not present in the changer device'''
    def __init__(self, tape):
        self.tape = tape
        
    def __str__(self):
        return 'Tape not found %s' % self.tape


class NoAvailableSpaceError(Exception):
    '''Indicates there is no space to load/unload media to.'''
    

class MediaInUseError(Exception):
    '''Indicates the requested media is already in use.'''


class Changer(object):
    def __init__(self, scsi_dev, drives):
        '''Control the given media changer. 
        
        Parameters
        ----------
        scsi_dev : ScsiDev
            The SCSI device corresponding to the changer

        drives : list
            A list of Drive objects associated with the changer. Should be in
            the order of their indices reported by 'mtx'.
        '''
        self.scsi_dev = scsi_dev
        self._drives = drives
        self._slots = None
        
        self._status_dirty = True

    def _parse_barcode(self, vals):
        status_toks = vals[0].split()
        if status_toks[0] == 'Full':
            if len(vals) == 2:
                match = re.match('VolumeTag *= *(.+)', vals[1])
                barcode = match.groups()[0]
            else:
                barcode = ''
        else:
            assert status_toks[0] == 'Empty'
            barcode = None
        return barcode

    def _update_status(self):
        stdout, _ = mtx_command(self, ['status'])

        n_drives = None
        n_slots = None
        for line in stdout.split('\n'):
            line = line.strip()
            if line == '':
                continue

            tokens = [x.strip() for x in line.split(':')]
            assert 1 < len(tokens) <= 3
            key = tokens[0]
            vals = tokens[1:]

            if key.startswith('Storage Changer'):
                assert len(vals) == 1
                assert n_drives is None and n_slots is None
                match = re.match('([0-9]+) Drives, ([0-9]+) Slots.+', vals[0])
                n_drives, n_slots = match.groups()
                n_drives, n_slots = int(n_drives), int(n_slots)
                if self._slots is None:
                    self._slots = [Slot() for i in range(n_slots)]
            elif line.startswith('Data Transfer Element'):
                drive_idx = int(key.split()[-1])
                barcode = self._parse_barcode(vals)
                self._drives[drive_idx].curr_tape = barcode
            elif line.startswith('Storage Element'):
                slot_idx = int(key.split()[-1]) - 1
                barcode = self._parse_barcode(vals)
                self._slots[slot_idx].curr_tape = barcode

        assert len(self._drives) == n_drives
        assert len(self._slots) == n_slots
        self._status_dirty = False
    
    @property
    @_needs_status
    def drives(self):
        return tuple(self._drives)
        
    @property
    @_needs_status
    def slots(self):
        return tuple(self._slots)
    
    @property
    def tapes(self):
        result = []
        for drive in self.drives:
            if drive.curr_tape is not None:
                result.append(drive.curr_tape)
        for slot in self.slots:
            if slot.curr_tape is not None:
                result.append(slot.curr_tape)
        return result
    
    @_dirties_status
    @_needs_status
    def load(self, barcode, drive=None):
        '''Load the tape into a drive, if it is not already in one, and 
        return the containing drive. 
        
        Raises an UnknownMediaError if the tape is not in any drive or slot. 
        Raises an NoAvailableSpaceError if there are no empty drives.
        '''
        empty_drive = None
        for idx, d in enumerate(self._drives):
            if d.curr_tape == barcode:
                if drive is not None and d != drive:
                    # A specific drive was requested but the tape is already 
                    # in a different drive
                    raise MediaInUseError()
                return d
            elif d == drive:
                if not d.curr_tape is None:
                    raise ValueError("Specified drive is not empty")
                drive_idx = idx
            elif empty_drive is None and d.curr_tape is None:
                empty_drive = d
                empty_idx = idx
        if drive is None:
            if empty_drive is None:
                raise NoAvailableSpaceError("No empty drives")
            drive = empty_drive
            drive_idx = empty_idx
        
        #Find the slot idx for the tape
        slot_idx = 1
        for slot in self._slots:
            if slot.curr_tape == barcode:
                break
            slot_idx += 1
        else:
            raise UnknownTapeError(barcode)

        # Perform the load operation and update the drive status. We sleep for 
        # five seconds after the load to allow the tape drive time to 
        # recognize that a tape is loaded.
        mtx_command(self, ['load', str(slot_idx), str(drive_idx)])
        time.sleep(5)
        drive.curr_tape = barcode
        slot.curr_tape = None
        
        return drive
    
    @_dirties_status
    @_needs_status
    def unload(self, drive, slot=None):
        '''Unload the contents of the drive into a slot.
        '''
        if drive.curr_tape is None:
            raise ValueError("The drive is already empty")
        drive_idx = self._drives.index(drive)
        
        if slot is not None:
            if slot.curr_tape is not None:
                raise ValueError("The specified slot is not empty")
            slot_idx = self._slots.index(slot) + 1
        else:
            slot_idx = 1
            for slot in self._slots:
                if slot.curr_tape == None:
                    break
                slot_idx += 1
            else:
                raise NoAvailableSpaceError("No empty slots")
        
        mtx_command(self, ['unload', str(slot_idx), str(drive_idx)])
        slot.curr_tape = drive.curr_tape
        drive.curr_tape = None
        
    def mark_dirty(self):
        self._status_dirty = True
        for drive in self.drives:
            drive.mark_dirty()


def get_changers(drive_type=None):
    '''Get a list of media changers available on this system.
    
    Tries to automatically deterimine the scsi devices for the drives that 
    belong to each changer device. The `host` and `channel` parts if the 
    SCSI paths must match. A ValueError will be raised if two changer devices 
    have the same `host` and `channel` parts of their SCSI path.
    '''
    scsi_devs = get_scsi_devs()
    changer_devs = {}
    for dev in scsi_devs:
        if dev.dev_type == 'mediumx':
            changer_devs[dev] = []
    
    # Throw an error if their is ambiguity over drive ownership among changer
    # devices
    for ch_dev1 in changer_devs.keys():
        for ch_dev2 in changer_devs.keys():
            if (ch_dev1 != ch_dev2 and 
                ch_dev1.scsi_path[:2] == ch_dev2.scsi_path[:2]
               ):
                raise ValueError("Cannot differentiate drive ownership "
                                 "between the changer devices %s and %s" % 
                                 (ch_dev1, ch_dev2)
                                )
    
    for dev in scsi_devs:
        if drive_type is None or drive_type == dev.dev_type:
            for changer_dev, drive_devs in changer_devs.iteritems():
                if (changer_dev != dev and 
                    changer_dev.scsi_path[:2] == dev.scsi_path[:2]
                   ):
                    drive_devs.append(dev)
    
    changers = []
    for changer_dev, drive_devs in changer_devs.iteritems():
        drives = []
        for drive_dev in drive_devs:
            drives.append(Drive(drive_dev))
        if len(drives) > 0:
            changers.append(Changer(changer_dev, drives))
    return changers
