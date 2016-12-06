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

fW = open('sentimentRankedWordList.txt','r')
all=fW.readlines()
head = all[0].strip().split('\t')
words = [line.strip().split('\t') for line in all]
wordMap = {line[0]:{head[1]:line[1], head[2]:line[2], head[3]:line[3], head[4]:line[4]} for line in words[1:]}
wmBroadcast = sc.broadcast(wordMap)

f = sc.textFile("/data/archive/*/part-0000*")

#More compact
messages = f.map(lambda x: json.loads(x))\
	    .map(lambda x: x["tweets"])\
	    .flatMap(lambda x: [[i["message"]["body"],\
            	i["cde"]["author"]["location"],\
            	i["message"]["postedTime"],\
            	i["cde"]["content"]["sentiment"]["polarity"],\
            	sum([float(wmBroadcast.value.get(word).get("happiness_average")) for word in i["message"]["body"].split() if word in wmBroadcast.value])] for i in x])

#Form: {tweetBody,[city,country,state], "dateTime", "polarity","happiness"}
hillary = messages.filter(lambda x: any(word in x[0] for word in ["hillary","clinton","rodham"]))\
					.flatMap(lambda x: [(x[2].split('T')[0] + i , [1, x[3],x[4], x[2], i]) for i in set(["United States"]+x[1].values())])\
					.reduceByKey(lambda a, b: (a[0]+b[0],[a[1],b[1]],a[2]+b[2], a[3],a[4]))

trump = messages.filter(lambda x: any(word in x[0] for word in ["donald","trump"]))\
					.flatMap(lambda x: [(x[2].split('T')[0] + i , [1, x[3],x[4], x[2], i]) for i in set(["United States"]+x[1].values())])\
					.reduceByKey(lambda a, b: (a[0]+b[0],[a[1],b[1]],a[2]+b[2], a[3],a[4]))

political = messages.filter(lambda x: any(word in x[0] for word in ["politics","political","election","vote","crooked","rally","debate","liberal","progressive","conservative","republican","leftwing","rightwing","democrat","alt-right"]))\
					.flatMap(lambda x: [(x[2].split('T')[0] + i , [1, x[3],x[4], x[2], i]) for i in set(["United States"]+x[1].values())])\
					.reduceByKey(lambda a, b: (a[0]+b[0],[a[1],b[1]],a[2]+b[2], a[3],a[4]))


hillary.saveAsTextFile("/data/jobOutputs/hillary")
trump.saveAsTextFile("/data/jobOutputs/trump")
political.saveAsTextFile("/data/jobOutputs/political")

pprint(hillary.take(1))
