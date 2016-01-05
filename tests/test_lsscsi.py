import os, sys
import nose
from nose.tools import eq_, ok_

# Make sure we test the local source code rather than the installed copy
test_dir = os.path.dirname(__file__)
src_dir = os.path.normpath(os.path.join(test_dir, '..'))
sys.path.insert(0, src_dir)
from tapeworm import lsscsi

# Find our test data
test_data_path = os.path.join(test_dir, 'data', 'lsscsi_stdout.txt')
with open(test_data_path, 'r') as f:
    test_data = f.read()

#Define the expected result
expected = [lsscsi.ScsiDev(None,
                           '/dev/sg0',
                           'enclosu',
                           lsscsi.ScsiPath(0, 0, 32, 0),
                           lsscsi.ScsiModel('DP', 'BACKPLANE', '1.07')
                          ),
            lsscsi.ScsiDev('/dev/sda',
                           '/dev/sg1',
                           'disk',
                           lsscsi.ScsiPath(0, 2, 0, 0),
                           lsscsi.ScsiModel('DELL', 'PERC H700', '2.10')
                          ),
            lsscsi.ScsiDev('/dev/sr0',
                           '/dev/sg12',
                           'cd/dvd',
                           lsscsi.ScsiPath(1, 0, 0, 0),
                           lsscsi.ScsiModel('TEAC', 'DVD-ROM DV-28SW', 'R.2A')
                          ),
            lsscsi.ScsiDev('/dev/st0',
                           '/dev/sg13',
                           'tape',
                           lsscsi.ScsiPath(5, 0, 0, 0),
                           lsscsi.ScsiModel('QUANTUM', 'ULTRIUM 6', '4142')
                          ),
            lsscsi.ScsiDev('/dev/sch0',
                           '/dev/sg14',
                           'mediumx',
                           lsscsi.ScsiPath(5, 0, 0, 1),
                           lsscsi.ScsiModel('QUANTUM', 'UHDL', '0090')
                          ),
           ]

def test_lsscsi():
    result = lsscsi.parse_lsscsi(test_data)
    eq_(result, expected)
