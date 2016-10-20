import xml.etree.ElementTree as ET
from difflib import SequenceMatcher
import re

import psycopg2

conn = psycopg2.connect(database="travel_info", user="postgres", password="pass", host="localhost", port="5432")
cur = conn.cursor()

cur.execute("SELECT country, threat_type FROM Twitter_Data")

all_data = cur.fetchall()

print all_data


conn.commit()
conn.close()

