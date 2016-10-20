import psycopg2
import sys


#connect to the database
conn=psycopg2.connect(database='tcount', user="postgres", password="pass", host="localhost", port="5432")
curs=conn.cursor()

#if there is no argument on the command line get the words and counts in ascending order
if len(sys.argv) == 1:
	sql = "SELECT word, count FROM tweetwordcount ORDER by lower(word) ASC;"
	curs.execute(sql)

	#put the results in a results array
	recs = curs.fetchall()

	#build the output string by looping through the records
	prstr = ""
	for rec in recs:
		prstr += "(" + rec[0] + ", " + str(rec[1]) + "), "
	print prstr[:-2]

#if there is an argument on the command line get the count for the supplied word
elif len(sys.argv) == 2:
	word = sys.argv[1]

	sql = "SELECT count FROM tweetwordcount WHERE word='%s';"%(word)
	curs.execute(sql)

	recs = curs.fetchall()
	for rec in recs:
		cnt = int(rec[0])
	print 'Total number of occurrences of "%s": %d'%(word, cnt)
else:
	print "please enter zero or one argument"

conn.commit()
conn.close()
