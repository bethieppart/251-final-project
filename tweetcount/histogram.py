import psycopg2
import sys
import re

#connect to the database
conn=psycopg2.connect(database='tcount', user="postgres", password="pass", host="localhost", port="5432")
curs=conn.cursor()

#if there are inputs on the command line make sure they are both integers then assign them to the upper and lower ranges
if len(sys.argv) == 2:
	nrange = str.strip(sys.argv[1])
	
	#see if they are both integers
	if bool(re.match(r'^[0-9]+,[0-9]+$', nrange)):
		lowend = int(nrange.split(',')[0])
		highend = int(nrange.split(',')[1])
		if lowend > highend:
			temp = lowend
			lowend = highend
			highend = temp
		sql = "SELECT word, count FROM tweetwordcount WHERE count BETWEEN %s AND %s ORDER by count DESC;"%(str(lowend), str(highend))
		curs.execute(sql)
		recs = curs.fetchall()
		for rec in recs:
			print "%20s: %4s"%(rec[0], rec[1])

	else:
		print 'please input two integers on the command line'
else:
	print 'please input two integers on the command line'

conn.commit()
conn.close()
