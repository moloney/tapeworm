import re
from collections import namedtuple
from util import sp_exec, NonZeroReturnException


SenseResult = namedtuple('SenseResult', 'error_code sense_key add_code add_qual')


class MtxError(NonZeroReturnException):
    def __init__(self, cmd, return_code, stdout, stderr, sense_result):
        super(MtxError, self).__init__(cmd, return_code, stdout, stderr)
        self.sense_result = sense_result

    def __str__(self):
        return ("MtxError: The command %s returned %d (sense_result=%s)\n"
                "stdout: %s\nstderr: %s") % \
                    (self.cmd,
                     self.return_code,
                     self.sense_result,
                     self.stdout,
                     self.stderr)


def parse_sense_result(stderr):
    error_code, sense_key, add_code, add_qual = (None, None, None, None)
    for line in stderr.split('\n'):
        line = line.strip()
        if line == '':
            continue
        match = re.match('mtx: Request Sense: ((?:\w|\s)+)=(.+)', line)
        if match:
            key, val = [x.strip() for x in match.groups()]
            if key == 'Error Code':
                hex_str = re.match('([A-F0-9]+).*', val).groups()[0]
                error_code = int(hex_str, 16)
            if key == 'Sense Key':
                sense_key = val
            if key == 'Additional Sense Code':
                hex_str = re.match('([A-F0-9]+).*', val).groups()[0]
                add_code = int(hex_str, 16)
            if key == 'Additional Sense Qualifier':
                hex_str = re.match('([A-F0-9]+).*', val).groups()[0]
                add_qual = int(hex_str, 16)
    return SenseResult(error_code, sense_key, add_code, add_qual)


def mtx_command(changer, cmd_args):
    full_cmd = ['mtx', '-f', changer.scsi_dev.gen_dev] + cmd_args
    try:
        return sp_exec(full_cmd)
    except NonZeroReturnException, e:
        sense_result = parse_sense_result(e.stderr)
        raise MtxError(e.cmd,
                       e.return_code,
                       e.stdout,
                       e.stderr,
                       sense_result
                      )

