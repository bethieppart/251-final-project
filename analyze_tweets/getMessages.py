import json
import os
import sys
from pprint import pprint

#Change SPARK_HOME to point to the folder where you installed Spark
spark_home = os.environ['SPARK_HOME'] = '/usr/local/spark'

if not spark_home:
    raise ValueError('SPARK_HOME enviroment variable is not set')
sys.path.insert(0,os.path.join(spark_home,'python'))
sys.path.insert(0,os.path.join(spark_home,'python/lib/py4j-0.9-src.zip'))
execfile(os.path.join(spark_home,'python/pyspark/shell.py'))


f = sc.textFile("/data/archive/file_20160417_0000/part-00000")
d = f.map(lambda x: json.loads(x))
blah = d.map(lambda x: x["tweets"]).map(lambda y: y)
all = sc.parallelize(blah.take(1)[0])
messages = all.map(lambda x: x["message"]["body"])
pprint(messages.take(10))
