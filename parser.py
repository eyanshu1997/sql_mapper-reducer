from flask import Flask, request
from flask_restful import Resource, Api, reqparse
import sqlparse
import re
import json
import time
import os
import subprocess

app = Flask(__name__)
api = Api(app)
app.config['SECRET_KEY'] = 'hello'

query = ""
ps = reqparse.RequestParser()
ps.add_argument('query')
def write_to_file(data):
	fil=open("/home/aurav/code/python/hadoop/pro/mapperip.txt","w")
	json.dump(data,fil)
#	fil.wrtie("\n");
	fil.close()
	return data
	
def extractlhs_and_rhs(exp):
	al_op = ['<=', '<', '==', '>', '>=','!=',"="]
	for o in al_op:
		if exp.find(o)!=-1:
			lhs,rhs=exp.split(o)
			return lhs.rstrip().lstrip(),o,rhs.rstrip().lstrip()
			
			
def extractfunccol(lhs):
	agg=["SUM","COUNT","MAX","MIN","AVG"]
	hav=None
	col=None
	for y in agg:
		if y in lhs:
			a,b=re.split(y,lhs)
			e,f=re.split("\(",b)
			c,d=re.split("\)",f)
			return y,c
			
					
def spark():
	command="/home/aurav/hadoop-3.3.0/spark-3.0.1-bin-hadoop3.2/bin/spark-submit /home/aurav/code/python/hadoop/pro/spark_run.py"
	os.system(command)
	#fi=open("/home/aurav/code/python/hadoop/pro/sparkresult.csv","r")
	#res=""
	#for li in fi:
	#	res.append(li)
	#return res
	return "ye"
	
def run_cmd(table):
	#command = "/home/aurav/hadoop-3.3.0/bin/hadoop jar /home/aurav/hadoop-3.3.0/share/hadoop/tools/lib/hadoop-streaming-3.3.0.jar -file ~/code/python/hadoop/pro/mapper.py    -mapper  ~/code/python/hadoop/pro/mapper.py -file  ~/code/python/hadoop/pro/reducer.py -reducer  ~/code/python/hadoop/pro/reducer.py -input /user/hadoop/{ip}.txt -output /user/hadoop/out".format(ip=table)
	#print(command)
	command = "/home/aurav/hadoop-3.3.0/bin/hadoop jar /home/aurav/hadoop-3.3.0/share/hadoop/tools/lib/hadoop-streaming-3.3.0.jar -mapper ~/code/python/hadoop/pro/mapper.py -reducer ~/code/python/hadoop/pro/reducer.py -input /user/hadoop/{ip}.txt -output /user/hadoop/out".format(ip=table)
	start= time.time()
	os.system(command)
	time_delta = time.time() - start
	cat = subprocess.Popen(["/home/aurav/hadoop-3.3.0/bin/hadoop", "fs", "-cat", "/user/hadoop/out/part-00000"], stdout=subprocess.PIPE)
	res=""
	for line in cat.stdout:
		temp_dict =  line.decode('utf-8').rstrip()
		#temp_dict = dict(zip(col_names, line.decode('utf-8').split('\n')[0].replace(",", "\t").split('\t')))	
		#print(temp_dict)
		res+=str(temp_dict)+"\n";
	cmd="/home/aurav/hadoop-3.3.0/bin/hdfs dfs -rm /user/hadoop/out/*"
	os.system(cmd)
	cmd="/home/aurav/hadoop-3.3.0/bin/hdfs dfs -rmdir /user/hadoop/out"
	os.system(cmd)
	#print(res)
	return res
#	return "yes"
	
def parse_query(query):
	parsed=sqlparse.parse(query.upper().rstrip().lstrip())
	stmt=parsed[0]
	columns=stmt.tokens[2]
	tables=stmt.tokens[6]
	where=stmt.tokens[8]
	group=stmt.tokens[11]
	compar=stmt.tokens[-1]
	where=str(where)
	#print(where)
	wh,where=where.split()
	if(where=="*"):
		wlhs="*"
		wo="*"
		wrhs="*"
	else:
		#print(where)
		wlhs,wo,wrhs=extractlhs_and_rhs(str(where))
		#print(lhs,o,rhs)
	columns=str(columns)
	columns=columns.split(",")
	column=[]
	#print(compar)
	compar=str(compar)
	if(compar=="*"):
		func="*"
		add="*"
		o="*"
		rhs="*"
		agg="*"
	else:
		#print(where)
		#wlhs,wo,wrhs=extractlhs_and_rhs(str(where))
		#print(lhs,o,rhs)
		lhs,o,rhs=extractlhs_and_rhs(compar)
		func,agg=extractfunccol(lhs)
	for co in columns:
		column.append(co.lstrip().rstrip())
	data={"columns":column, "tables":str(tables).lstrip().rstrip(), "wlhs":wlhs, "wo":wo, "wrhs":wrhs, "group":str(group).lstrip().rstrip(), "func":func, "agg":agg, "op":o, "rhs":rhs}
	write_to_file( data)
	table=str(tables).lstrip().rstrip().lower()
	#result=run_cmd(table)
	res=spark()
	#result=""
	#return result+res
	return "yes"
	
	#return data
class RunQuery(Resource):
	def get(self):
		return("use post method")
	def post(self):
		args = ps.parse_args()
		query = args['query']
		return parse_query(query)
api.add_resource(RunQuery, '/query')
app.run(debug=True)
