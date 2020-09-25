#!/usr/bin/python

import sys
import json
import operator
import re
f2=open("mapperip.txt")
l=f2.readline()
config=json.loads(l)
res={}
havar={}
hav=config["agg"]
func=config["func"]
op=config["op"]
#print(config)
intval=["PID","VOTES","HELPFUL","RATING","SALESRANK"]
def operate(v,ope):
	if(ope=="SUM"):
		return sum(v)
	if(ope=="MIN"):
		return min(v)
	if(ope=="MAX"):
		return max(v)
	if(ope=="COUNT"):
		return len(v)
	if(ope=="AVG"):
		return sum(v)/len(v)
def oper(val):
	choice={ '<':  operator.lt,'<=': operator.le,'>':  operator.gt,'>=': operator.ge,'==': operator.eq,'!=': operator.ne}
	rhs=str(config["rhs"])
	if hav in intval:
		rhs=int(rhs)
		val=int(val)
	#print(val,rhs)
	#print(type(val),type(rhs))
	return choice[op](val,rhs)
	

		
for line in sys.stdin:
	line = line.strip()
	line = line.split('\t')
	#print(line)
	if line[2] in res:
		havar[int(line[2])].append(line[1])
		res[int(line[2])].append(line[0])
	else:
		havar[int(line[2])] = [str(line[1])]
		res[int(line[2])] = [str(line[0])]
#print(res)
ope=None
col=None
agg=["SUM","COUNT","MAX","MIN","AVG"]

for k, v in havar.items():
	#print(val)
	val=operate(v,func)
	if oper(val):
		print("{}\t{}\t {}".format(k,",".join(res[k]), val))
		#print("yes")
		
			

