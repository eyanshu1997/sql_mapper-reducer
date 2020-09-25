#  SELECT group, COUNT(group) FROM products GROUP BY group HAVING COUNT(group)>3
f=open("resultjson.txt")
import json
i=0;
f2=open("mapperip.txt")
l=f2.readline()
config=json.loads(l)
#print(config['columns'])

sel=config['columns']
agg_col=config['group']
for li in f:
	if(i>1):
		exit()
	line=json.loads(li)
	i=i+1
	print(line)
	if line['title']=="discontinued product":
		continue
	sel_cols = [line[x] for x in sel]
	agg_cols = line[agg_col]
	print("{}\t{}".format(",".join(sel_cols), agg_cols))
"""

# query = 'SELECT MOVIEID,SUM(RATING) FROM RATING GROUPBY MOVIEID HAVING SUM(RATING) > 1000'
