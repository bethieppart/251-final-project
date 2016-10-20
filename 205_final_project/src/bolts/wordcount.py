from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt
import string
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
        country = tup.values[3]
        coords = tup.values[4]

        printable = set(string.printable)
        tweet = filter(lambda x: x in printable, tweet)
        location = filter(lambda x: x in printable, location)
        # Write codes to increment the word count in Postgres
        # Use psycopg to interact with Postgres
        # Database name: Tcount 
        # Table name: Tweetwordcount 
        # you need to create both the database and the table in advance.

        try:
            conn = psycopg2.connect(database="travel_info", user="postgres", password="pass", host="localhost", port="5432")

            cur = conn.cursor()

            cur.execute("INSERT INTO Twitter_Data (tweet, location, country, coordinates, threat_type) VALUES ( %s, %s, %s, %s, %s)", (tweet, location, country, coords, category))

            conn.commit()
            conn.close()

            self.counts[category] += 1
            self.emit([category, location, self.counts[category], tweet, country, coords])
        except:
            self.log("Cannot log tweet...")

        # Log the count - just to see the topology running
        try:
            self.log('%s: %s: %d %s %s' % (category, location, self.counts[category], country, coords))
            self.log(tweet)
        except:
            pass

