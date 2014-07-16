import tarfile, gzip
from datetime import datetime
import pathmap
from pathmap import PathMap
from tapeworm import archive
from tapeworm.archive import gen_archives

pm = PathMap()
arc_gen = gen_archives(pm.matches('/tmp/bench'), 4*1024**3)
next(arc_gen)

f_obj = gzip.open('/tmp/test_python_cmplvl_6.tar.gz', 'w', compresslevel=6)
data_tf = tarfile.open(fileobj=f_obj, mode='w')
start = datetime.now()
arc_gen.send((data_tf, None))
data_tf.close()
f_obj.close()
end = datetime.now()
print end - start
