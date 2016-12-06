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
#jsonGnip = f.map(lambda x: json.loads(x))
#jsonTweetsEmbedded = jsonGnip.map(lambda x: x["tweets"])
#jsonTweets = sc.parallelize(jsonTweetsEmbedded.take(10000))
#twts = jsonTweetsEmbedded.map(lambda x: x[0]["message"]["body"])
#messages = twts.map(lambda x: x[0]["message"]["body"])
#pprint(messages.take(10000))
#print len(messages.take(10000))

#More compact
messages = f.map(lambda x: json.loads(x))\
	    .map(lambda x: x["tweets"])\
	    .flatMap(lambda x: [[i["message"]["body"],i["cde"]["content"]["sentiment"]["polarity"]] for i in x])
sentiment = messages.map(lambda x: [x[0], x[1], sum([float(wmBroadcast.value.get(word).get("happiness_average")) for word in x[0].split() if word in wmBroadcast.value])])
proof = messages.take(int(sys.argv[1]))
pprint(proof)
pprint(len(proof))
pprint(sentiment.take(int(sys.argv[1])))
