from util import sp_exec, NonZeroReturnException


class TapeAlert(object):
    '''Object representing a single tape alert'''
    def __init__(self, flag_num, flag_text):
        self.flag_num = flag_num
        self.flag_text = flag_text

    def __str__(self):
        return 'TapeAlert[%d]: %s' % (self.flag_num, self.flag_text)
        
    def __eq__(self, other):
        return self.flag_num == other.flag_num
        
        
class UnhandledTapeAlertError(Exception):
    '''Raised when one or more tape alerts are not handled.
    
    Parameters
    ----------
    unhandled : list
        List of tuples consisting of a (scsi_dev, barcode) tuple from the 
        drive and a list of tape alerts.
    '''
    def __init__(self, unhandled, tape_info):
        self.unhandled = unhandled
        self.tape_info = tape_info
        
    def __str__(self):
        result = []
        for drive_info, alerts in self.unhandled:
            result.append('Unhandled tape alerts on dev %r with tape %s:' % 
                          drive_info)
            result.extend('\t%s' % a for a in alerts)
        return '\n'.join(result)


CLEAN_ALERT_FLAGS = set((20, 21))
'''Tape alert flags that indicate the drive is requesting cleaning.'''


EXPIRED_CLEAN_ALERT_FLAG = 22
'''Tape alert flag that indicates that the cleaning cartridge is expired.'''


WARN_ALERT_FLAGS = set((1, 2, 3, 7, 8, 15, 17, 18))
'''Tape alert flags that indicate a non-fatal warning.'''


IS_CLEAN_CART_ALERT_FLAG = 11
'''Tape alerts indicating that a cleaning cartridge is currently loaded.'''


def parse_tape_info(stdout):
    info = {}
    alerts = []
    for line in stdout.split('\n'):
        line = line.strip()
        if line == '':
            continue
        first_colon = line.index(':')
        key = line[:first_colon].strip()
        val = line[first_colon+1:].strip()
        if key.startswith('TapeAlert['):
            flag_num = int(key[10:-1])
            alerts.append(TapeAlert(flag_num, val))
        else:
            #If the value has quotes strip them and any interior white space
            if val[0] == "'" and val[-1] == "'":
                val = val[1:-1].strip()
            else:
                #Try to convert to an integer
                try:
                    if val[:2] == '0x':
                        val = int(val, 16)
                    else:
                        val = int(val)
                except ValueError:
                    pass
                
            info[key] = val

    return (info, alerts)


def get_tape_info(drive):
    '''Get the results from the 'tapeinfo' command, including any tape alerts.

    Parameters
    ----------
    drive : devctrl.Drive
        The drive to get info from

    Returns
    -------
    std_info : dict
        Any standard info returned by 'tapeinfo'

    tape_alerts : list
        List of any TapeAlerts returned by 'tapeinfo'
    '''
    cmd = ['tapeinfo', '-f', drive.scsi_dev.gen_dev]
    stdout, _ = sp_exec(cmd)
    return parse_tape_info(stdout)
