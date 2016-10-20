from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt

import psycopg2

def ascii_string(s):
  return all(ord(c) < 128 for c in s)

class WordCounter(Bolt):

    def initialize(self, conf, ctx):
        self.counts = Counter()
        # self.redis = StrictRedis()

    def process(self, tup):
        tweet = tup.values[0]
        category = tup.values[1]
        location = tup.values[2]

        # Write codes to increment the word count in Postgres
        # Use psycopg to interact with Postgres
        # Database name: Tcount 
        # Table name: Tweetwordcount 
        # you need to create both the database and the table in advance.
        '''
        conn = psycopg2.connect(database="tcount", user="postgres", password="pass", host="localhost", port="5432")

        cur = conn.cursor()

        cur.execute("SELECT word, count from Tweetwordcount WHERE word='%s'" % word)

        tup = cur.fetchone()

        #if it's a new word, add it to the db
        if tup is None:
            self.counts[word] += 1
            try:
                cur.execute("INSERT INTO Tweetwordcount (word, count) VALUES ('%s', %s)" % (word, self.counts[word]))
            
            except KeyboardInterrupt:
                raise

            except:
                self.log('db collision on %s, using UPDATE instead!' %word)
                cur.execute("UPDATE Tweetwordcount SET count='%s' WHERE word='%s'" % (self.counts[word]+1, word))

        #else update the existing record
        else:
            self.counts[word] = tup[1]+1
            cur.execute("UPDATE Tweetwordcount SET count='%s' WHERE word='%s'" % (self.counts[word], word))

        conn.commit()
        conn.close()
        '''
        self.counts[category] += 1
        self.emit([category, location, self.counts[category], tweet])

        # Log the count - just to see the topology running
        try:
            self.log('%s: %s: %d\n%s' % (category, location, self.counts[category], tweet))

        except:
            pass

