#!/usr/bin/python3

import re
#  SELECT group, COUNT(group) FROM products GROUP BY group HAVING COUNT(group)>3
f=open("resultjson.txt")
import json
i=0;
f2=open("mapperip.txt")
l=f2.readline()
config=json.loads(l)
#print(config['columns'])

sel=config['columns']

#print(sel)
se=[]
agg_col=config['group']
hav=config['agg']
for x in sel:
	if hav not in x:
		se.append(x)
#se=[x in sel if x!=agg_col]
#print(se)
#print(hav)
#print(agg_col)
import sys
for li in sys.stdin:
	if(i==200):
		exit()
	line=json.loads(li)
	i=i+1
	#print(line)
	if line['TITLE']=="discontinued product":
		continue
	sel_cols = [line[x] for x in se]
	agg_cols = line[agg_col]
	ha=line[hav]
	print("{}\t{}\t{}".format(",".join(sel_cols),ha, agg_cols))

# query = 'SELECT MOVIEID,SUM(RATING) FROM RATING GROUPBY MOVIEID HAVING SUM(RATING) > 1000'
