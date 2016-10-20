import psycopg2
import sys


#connect to the database
conn=psycopg2.connect(database='tcount', user="postgres", password="pass", host="localhost", port="5432")
curs=conn.cursor()

sql = "SELECT word, count FROM tweetwordcount ORDER by count DESC LIMIT 20;"
curs.execute(sql)

#put the results in a results array
recs = curs.fetchall()

print recs

conn.commit()
conn.close()
