from flask import Flask, request
from flask_restful import Resource, Api, reqparse
import sqlparse
import re
import json


app = Flask(__name__)
api = Api(app)
app.config['SECRET_KEY'] = 'hello'

query = ""
ps = reqparse.RequestParser()
ps.add_argument('query')
def write_to_file(data):
	fil=open("mapperip.txt","w")
	json.dump(data,fil)
	fil.close()
	return data
def extractlhs_and_rhs(exp):
	al_op = ['<=', '<', '=', '>', '>=']
	for o in al_op:
		if exp.find(o)!=-1:
			rhs,lhs=exp.split(o)
			return {"lhs":lhs.rstrip(),"op":o,"rhs":rhs.rstrip()}
		
def parse_query(query):
	parsed=sqlparse.parse(query.upper())
	stmt=parsed[0]
	columns=stmt.tokens[2]
	tables=stmt.tokens[6]
	where=stmt.tokens[8]
	compar=stmt.tokens[-1]
	where=str(where)
	whe,group=re.split("GROUPBY ",where)
	wh,where=whe.split()
	if(where=="*"):
		where="*"
	else:
		where=extractlhs_and_rhs(str(where))
	columns=str(columns)
	columns=columns.split(",")
	column=[]
	compar=extractlhs_and_rhs(str(compar))

	for co in columns:
		column.append(co)
	#print(column)
	#print(tables)
	#print(where)
	#print(group)
	#print(compar) 
	data={"columns":str(column),"tables":str(tables),"where":str(where),"group":str(group),"compar":str(compar)}
	write_to_file( data)
	#command = 'hadoop jar {hadoop_streaming_jar} -mapper "python mrmapper/groupby_mapper.py" -reducer "python mrmapper/groupby_reducer.py" -input /{input}/{table}.csv -output /{parent}/{outputdir}'.format(table=queryset.fromtable, input=self.input, parent=self.parentdir, hadoop_streaming_jar=self.hadoop_streaming_jar, outputdir=self.outputdir)
	#start= time.time()
	#os.system(command)
	#time_delta = time.time() - start
	return data

class RunQuery(Resource):
    def get(self):
        pass

    def post(self):
        args = ps.parse_args()
        query = args['query']
        return parse_query(query)
api.add_resource(RunQuery, '/query')
app.run(debug=True)
