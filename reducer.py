#!/usr/bin/python

import sys
import json
import operator
import re
f2=open("mapperip.txt")
l=f2.readline()
config=json.loads(l)
res={}
hav={}
#print(config)

def oper(val):
	choice={ '<':  operator.lt,'<=': operator.le,'>':  operator.gt,'>=': operator.ge,'==': operator.eq,'!=': operator.ne}
	op=config["op"]
	rhs=str(config["rhs"])
	#print(val,rhs)
	#print(type(val),type(rhs))
	return choice[op](val,rhs)
	
def op(v,ope):
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
		
for line in sys.stdin:
	line = line.strip()
	line = line.split('\t')
	#print(line)
	if line[2] in res:
		hav[line[2]].append(line[1])
		res[line[2]].append(line[0])
	else:
		hav[line[2]] = [line[1]]
		res[line[2]] = [line[0]]
#print(res)
ope=None
col=None
agg=["SUM","COUNT","MAX","MIN","AVG"]
lhs=config["lhs"]
set=False
for y in agg:
	if y in lhs:
		a,b=re.split(y,lhs)
		set=True
		e,f=re.split("\(",b)
		c,d=re.split("\)",f)
		ope=y
		col=c
		break
if col==None or ope==None:
	print("error")
	exit()
for k, v in hav.items():
	val=op(v,ope)
	#print(val)
	if oper(val):
		print("{}\t{}\t {}".format(k,",".join(res[k]), val))
		#print("yes")
		
			

