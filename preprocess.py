import re
import json
elements=[]
def readelement(f):
	idl=f.readline()
	z=re.search("Id:   ",idl)
	while(z!=None):
		idl=f.readline()
		z=re.search(":   ",idl)
	idl=f.readline()
	#print("{"+idl+"}")
	x,id=re.split(":   ",idl)
	#print(id.rstrip())
	ail=f.readline()
	#print(ail)
	x,amazonid=re.split(": ",ail)
	#print(amazonid.rstrip())
	tl=f.readline()
	check=re.search("discontinued product",tl)
	if(check!=None):
		element={"PID":id.rstrip(),"ASIN":amazonid.rstrip(),"TITLE":"discontinued product"}
		return element
	x,title=re.split(": ",tl,1)
	#print(title.rstrip())
	gl=f.readline()	
	x,group=re.split(": ",gl)
	#print(group.rstrip())
	sl=f.readline()
	x,srank=re.split(": ",sl)
	#print(srank.rstrip())
	sil=f.readline()
	x,sim=re.split(": ",sil)
	tok=sim.split()
	simc=tok[0];
	#print(simc)
	similar=[]
	for i in range(int(simc)):
		similar.append(tok[i+1])
	#print(similar.rstrip())
	cil=f.readline()	
	x,c=re.split(": ",cil)
	#print(c.rstrip())
	categories=[]
	for i in range(int(c)):
		ce=f.readline()
		categories.append(ce.rstrip())
	#print(categories)
	cil=f.readline()	
	r,t,c,d,dc=re.split(": ",cil)
	#print(t)
	#print(c)
	#print("["+d+"]")
	#print(dc)
	#print(count)
	cound,download=re.split("  ",d);
	count,download=re.split("  ",c);
	#print(cound)
	#print(count)
	#if cound!=count:
	#	print("error")
	#	exit()
	reviews=[]
	for i in range(int(cound)):
		rev=f.readline()
		ti,cus,ra,vo,he=re.split(": ",rev)
		time,x=ti.split();
		customer,x=cus.split();
		rating,x=ra.split()
		votes,x=vo.split();
		helpful=he.rstrip();
		#print(time,customer,rating,votes,helpful)
		
		reviews.append({"ASIN":amazonid.rstrip(),"TIME":time,"PID":id.rstrip(),"USERID":customer,"RATING":rating,"VOTES":votes,"HELPFUL":helpful})
	#print(reviews)
	element={"PID":id.rstrip(),"ASIN":amazonid.rstrip(),"TITLE":title.rstrip(),"GROUP":group.rstrip(),"SALESRANK":srank.rstrip(),"SIMILAR":similar,"CATEGORIES":categories,"REVIEWS":reviews}
	return element	
f=open("/home/aurav/Downloads/amazon-meta.txt","rt")

f.readline()
f.readline()
write=open("resultjson1.txt","w")
for i in range(548551):
	#f.readline()
	
	x=readelement(f)
	json.dump(x,write)
	write.write("\n")
#f.readline()
#readelement(f)

