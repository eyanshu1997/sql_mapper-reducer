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
#print(hav)
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
	choice={ '<':  operator.lt,'<=': operator.le,'>':  operator.gt,'>=': operator.ge,'==': operator.eq,'!=': operator.ne,"=":operator.eq}
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
		havar[str(line[2])].append(str(line[1]))
		res[str(line[2])].append(str(line[0]))
	else:
		havar[str(line[2])] = [str(line[1])]
		res[str(line[2])] = [str(line[0])]
#print(res)
ope=None
col=None
agg=["SUM","COUNT","MAX","MIN","AVG"]
if(hav=="*"):
	for k, v in res.items():
		if("*" in v):
			print(k)
		else:
			print("{}\t {}".format(k, v))
else:
	for k, v in havar.items():
		#print(val)
		val=operate(v,func)
		if oper(val):
			if("*"in res[k]):
				print("{}\t {}".format(k, val))			
			else:
				print("{}\t{}\t {}".format(k,",".join(res[k]), val))
		#print("yes")
		
			

