import os, sys
import nose
from nose.tools import eq_, ok_

# Make sure we test the local source code rather than the installed copy
test_dir = os.path.dirname(__file__)
src_dir = os.path.normpath(os.path.join(test_dir, '..'))
sys.path.insert(0, src_dir)
from tapeworm import tapeinfo

# Find our test data
test_data_path = os.path.join(test_dir, 'data', 'tapeinfo_stdout.txt')
with open(test_data_path, 'r') as f:
    test_data = f.read()

#Define the expected result
expected = ({'Product Type' : 'Tape Drive',
             'Vendor ID' : 'QUANTUM',
             'Product ID' : 'ULTRIUM 6',
             'Revision' : '4142',
             'Attached Changer API' : 'No',
             'SerialNumber' : 'XXXXXXXX',
             'MinBlock' : 1,
             'MaxBlock' : 16777215,
             'SCSI ID' : 0,
             'SCSI LUN' : 0,
             'Ready' : 'yes',
             'BufferedMode' : 'yes',
             'Medium Type' : 'Not Loaded',
             'Density Code' : 0x5a,
             'BlockSize' : 262144,
             'DataCompEnabled' : 'yes',
             'DataCompCapable' : 'yes',
             'DataDeCompEnabled' : 'yes',
             'CompType' : 0x1,
             'DeCompType' : 0x1,
             'Block Position' : 2438262,
             'ActivePartition' : 0,
             'EarlyWarningSize' : 0,
             'NumPartitions' : 0,
             'MaxPartitions' : 3,
            },
            [tapeinfo.TapeAlert(3, 'Hard Error: Uncorrectable read/write error.'),
             tapeinfo.TapeAlert(4, 'Media: Media Performance Degraded, Data Is At Risk.'),
             tapeinfo.TapeAlert(6, 'Write Failure: Tape faulty or tape drive broken.'),
             tapeinfo.TapeAlert(20, 'Clean Now: The tape drive neads cleaning NOW.'),
            ]
           )

def test_tape_info():
    result = tapeinfo.parse_tape_info(test_data)
    assert result[0] == expected[0]
    for idx, alert in enumerate(result[1]):
        assert alert.flag_num == result[1][idx].flag_num
        assert alert.flag_text == result[1][idx].flag_text
