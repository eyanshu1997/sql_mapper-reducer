from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import operator
#{"columns": ["PID", "ASIN", "TITLE"], "tables": "PRODUCT", "wlhs": "PID", "wo": "<", "wrhs": "7", "group": "PID", "func": "*", "agg": "*", "op": "*", "rhs": "*"}

def op_val(op):
	choice={ "<":  operator.lt,"<=": operator.le,">":  operator.gt,">=": operator.ge,"==": operator.eq,"!=": operator.ne,"=":operator.eq}
	return choice[op]

spark = SparkSession \
    .builder \
    .appName("Python Spark mapper") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()



columns=list()
tables=""
wlhs=""
wo=""
wrhs=""
group=""
func=""
agg=""
op=""
rhs=""
with open("/home/aurav/code/python/hadoop/pro/mapperip.txt","r") as file:
	jfile = json.load(file)
	columns=jfile["columns"]
	tables=jfile["tables"]
	wlhs=jfile["wlhs"]
	wo=jfile["wo"]
	wrhs=jfile["wrhs"]
	group=jfile["group"]
	func=jfile["func"]
	agg=jfile["agg"]
	op=jfile["op"]
	rhs=jfile["rhs"]

table_name="/home/aurav/code/python/hadoop/pro/data/"+tables.lower()+".txt"
intval=["CID","PID","VOTES","HELPFUL","RATING","SALESRANK"]
	
df = spark.read.format("json").option("header",True).load(table_name)
if wlhs in intval:
	df=df.withColumn(wlhs, df[wlhs].cast(IntegerType()))
	df=df.withColumn(agg, df[agg].cast(IntegerType()))
# WHERE CLAUSE	
if wo == "==" or wo == "=":
	df = df.filter(col(wlhs) == int(wrhs))
elif wo == ">":
	df = df.filter(col(wlhs) > int(wrhs))
elif wo == ">=":
	df = df.filter(col(wlhs) >= int(wrhs))
elif wo == "<":
	df = df.filter(col(wlhs) < int(wrhs))
elif wo == "<=":
	df = df.filter(col(wlhs) <= int(wrhs))
# WHERE ENDS

# SELECT CLAUSE
#print(type(df))
if "*" not in columns:
	c=[]
	for a in columns:
		if agg not in a:
			c.append(a)
		else:
			c.append(agg)
	df = df.select(c)

# SELECT ENDS
group_by_flag=0
# GROUPBY CLAUSE
if "*" != group:
	df = df.groupBy(group)
	group_by_flag=1
# GROUPBY ENDS

#HAVING CLAUSE
if group_by_flag:
	if "*" in op:
		if func == "SUM":
			df =df.agg(sum(agg))
			new_agg = func.lower() +"("+agg+")"
			if op != "*":
				df = df.where(op_val(op)(col(new_agg),int(rhs)))
		elif func == "MAX":
			df = df.agg(max(agg))
			new_agg = func.lower() +"("+agg+")"
			if op != "*":
				df = df.where(op_val(op)(col(new_agg),int(rhs)))
		elif func == "MIN":
			df = df.agg(min(agg))
			new_agg = func.lower() +"("+agg+")"
			if op != "*":
				df = df.where(op_val(op)(col(new_agg),int(rhs)))
		elif func == "COUNT":
			df = df.agg(count(agg))
			new_agg = func.lower() +"("+agg+")"
			if op != "*":
				df = df.where(op_val(op)(col(new_agg),int(rhs)))
	elif "*" not in op:
		if func == "SUM":
			df =df.agg(sum(agg))
		elif func == "MAX":
			df = df.agg(max(agg))
		elif func == "MIN":
			df = df.agg(min(agg))
		elif func == "COUNT":
			df = df.agg(count(agg))


df.show(truncate=False)
df.printSchema()
df.repartition(1).write.csv('/home/aurav/code/python/hadoop/pro/sparkresult.csv',sep='\t')

"""
df.filter(col("PID") == "1").show(truncate=False)
"""
