#!/usr/bin/python3
import sys
import operator
import re
#  SELECT group, COUNT(group) FROM products GROUP BY group HAVING COUNT(group)>3
#f=open("resultjson.txt")
import json

f2=open("/home/aurav/code/python/hadoop/pro/mapperip.txt")
l=f2.readline()
config=json.loads(l)
#print(config['columns'])

sel=config['columns']

#print(sel)
se=[]
agg_col=config['group']
hav=config['agg']
#se=[x in sel if x!=agg_col]
#print(se)
#print(hav)
#print(agg_col)
wlhs=config['wlhs']
wo=config['wo']
table=config["tables"]
wrhs=config['wrhs']
intval=["PID","VOTES","HELPFUL","RATING","SALESRANK"]

def oper(lhs,rhs,op):
	choice={ '<':  operator.lt,'<=': operator.le,'>':  operator.gt,'>=': operator.ge,'==': operator.eq,'!=': operator.ne,"=":operator.eq}
	return choice[op](lhs,rhs)
	

#i=0;	
def run(line):
	x=None
	y=None
	#print(line)
	if wlhs!="*":
		if(wlhs in intval):
			x=int(line[wlhs])
			y=int(wrhs)
		else:
			x=str(line[wlhs])
			y=str(wrhs)
		if(not oper(x,y,wo)):
			return	
	sel_cols = [line[x] for x in se]
	agg_cols = line[agg_col]
	if(hav=="*"):
		ha="*"
	else:
		ha=line[hav]
	if(len(se)==0):
		print("{}###{}\t{}".format("*",ha, agg_cols))
	else:
		print("{}###{}\t{}".format(",".join(sel_cols),ha, agg_cols))
		
	
for x in sel:
	if hav not in x:
		if x!= agg_col:
			se.append(x)
				
for li in sys.stdin:
#	if(i==200):
#		exit()
#	i=i+1
	line=json.loads(li)
	#print(line)
	if line['TITLE']=="discontinued product":
		continue
	if(table=="REVIEWS"):
		li=line['REVIEWS']
		for line in li:
			run(line)
	elif(table=="SIMILAR"):
		li=line['SIMILAR']
		for line in li:
			run(line)
	else:
		run(line)

# query = 'SELECT MOVIEID,SUM(RATING) FROM RATING GROUPBY MOVIEID HAVING SUM(RATING) > 1000'
