from collections import namedtuple
from util import sp_exec


ScsiPath = namedtuple('ScsiPath', 'host channel target lun')


ScsiModel = namedtuple('ScsiModel', 'manufacturer model firmware')


class ScsiDev(object):
    def __init__(self, spec_dev, gen_dev, dev_type, scsi_path, scsi_model):
        self.spec_dev = spec_dev
        self.gen_dev = gen_dev
        self.dev_type = dev_type
        self.scsi_path = scsi_path
        self.scsi_model = scsi_model

    def __repr__(self):
        return ('ScsiDev(spec_dev=%r, gen_dev=%r, dev_type=%r, scsi_path=%r,'
                ' scsi_model=%r)') % \
                   (self.spec_dev,
                    self.gen_dev,
                    self.dev_type,
                    self.scsi_path,
                    self.scsi_model,
                   )

    def __eq__(self, other):
        return repr(self) == repr(other)

    def __hash__(self):
        return hash(repr(self))


def parse_lsscsi(stdout):
    '''Parse the output of the command "lsscsi -g".'''
    devs = []
    for line in stdout.split('\n'):
        line = line.strip()
        if line == '':
            continue
        tokens = line.split()
        host, channel, target, lun = \
            [int(x) for x in tokens[0][1:-1].split(':')]
        dev_type = tokens[1]
        manufacturer = tokens[2]
        model = ' '.join(tokens[3:-3]) # This can have spaces
        firmware = tokens[-3]
        spec_dev = tokens[-2]
        gen_dev = tokens[-1]
        if spec_dev == '-':
            spec_dev = None
        if gen_dev == '-':
            gen_dev = None

        scsi_path = ScsiPath(host, channel, target, lun)
        scsi_model = ScsiModel(manufacturer, model, firmware)
        devs.append(ScsiDev(spec_dev,
                            gen_dev,
                            dev_type,
                            scsi_path,
                            scsi_model)
                   )

    return devs

def get_scsi_devs():
    '''Return a list of ScsiDev objects.'''
    stdout, _ = sp_exec(['lsscsi', '-g'])
    return parse_lsscsi(stdout)

