from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt
import psycopg2


class WordCounter(Bolt):

    def initialize(self, conf, ctx):
        self.counts = Counter()
        #self.redis = StrictRedis()

    def upsert(self, uWord, uCount):

	#remove apostrophes
	if "'" in uWord:
		uWord=uWord.replace("'", "''")

	#connect to the database
	conn=psycopg2.connect(database='tcount', user="postgres", password="pass", host="localhost", port="5432")
	curs=conn.cursor()

	#do the actual update
	sql = "UPDATE tweetwordcount SET count=%s WHERE word='%s';"%(uCount, uWord)
	sql += "INSERT INTO tweetwordcount (word, count) SELECT '%s', %s WHERE NOT EXISTS"%(uWord, uCount)
	sql += " (SELECT 1 FROM tweetwordcount WHERE word='%s');"%(uWord)

	curs.execute(sql)
	conn.commit()

	conn.close()

    def process(self, tup):
        word = tup.values[0]

        # Write codes to increment the word count in Postgres
        # Use psycopg to interact with Postgres
        # Database name: Tcount 
        # Table name: Tweetwordcount 
        # you need to create both the database and the table in advance.
        

        # Increment the count
        self.counts[word] += 1
        self.emit([word, self.counts[word]])

	# either update or insert
	self.upsert(word, self.counts[word])

        # Log the count - just to see the topology running
        self.log('%s: %d' % (word, self.counts[word]))
