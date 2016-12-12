import io
import os
import ast
import re
import csv
#import unicodecsv as csv
from subprocess import call 

global reg 
reg = re.compile('[a-zA-Z]')

#Considered this, but it's easier to just evaluate 'cat' from python 
def walkDirectory(rootDir):
	for dirName, subdirList, fileList in os.walk(rootDir):
		print "Found Directory: ", dirName
		for fname in fileList:
			print os.path.join(dirName, fname)			


call(["cat /data/jobOutputs/hillary/* > /data/jobOutputs/hillaryCombined"], shell=True)
call(["cat /data/jobOutputs/trump/* > /data/jobOutputs/trumpCombined"], shell=True)
call(["cat /data/jobOutputs/political/* > /data/jobOutputs/politicalCombined"], shell=True)

def parseToCsv():
	filesToTopics = {'/data/jobOutputs/hillaryCombined': 'hillary','/data/jobOutputs/trumpCombined': 'trump','/data/jobOutputs/politicalCombined': 'political'}
	sentimentScores = {"AMBIVALENT":0, "NEUTRAL":0, "UNSURE":0, "POSITIVE":1, "NEGATIVE":-1}
	rows = []
	for fname, topic in filesToTopics.iteritems():
		with open(fname, 'r') as f:
			lines = f.readlines()
			for line in lines:
				key, value = ast.literal_eval(line)
				date = reg.sub('', key)
				tweetCount = value[0]
				polarities = value[1] #Gotta do something here.  Maybe take the sum sum() sum([sentScores[p] for p in polarities])/len(polarities)
				polarityScore = float(sum([sentimentScores[p] for p in polarities]))/len(polarities)
				totalSentiment = value[2]
				avgSentiment = totalSentiment/tweetCount
				fullDate = value[3]
				location = value[4]
				label = topic
				row = [date, location, label, tweetCount, polarityScore, totalSentiment, avgSentiment, fullDate]
				row = [i.encode('utf-8') if isinstance(i, unicode) else i for i in row]
				rows.append(row)

	with open("/data/jobOutputs/CombinedOutput.csv", 'wb') as f:
		wr = csv.writer(f)#, quoting=csv.QUOTE_ALL)
		wr.writerows(rows)

parseToCsv()
