import os, sys
import nose
from nose.tools import *

# Make sure we test the local source code rather than the installed copy
test_dir = os.path.dirname(__file__)
src_dir = os.path.normpath(os.path.join(test_dir, '..'))
sys.path.insert(0, src_dir)
from tapeworm import mtx

# Find our test data
test_data_path = os.path.join(test_dir, 'data', 'mtx_sense_stderr.txt')
with open(test_data_path, 'r') as f:
    test_data = f.read()
    
#Define the expected result
expected = mtx.SenseResult(0x70, 'Not Ready', 0x30, 0x3)
    
def test_mtx():
    result = mtx.parse_sense_result(test_data)
    assert result == expected
