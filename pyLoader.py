import urllib2
from urllib2 import Request
import json    
import csv
import contextlib
import httplib
import requests
from timeit import default_timer as timer

overallstart = timer()

#CleanDB
payload = {"Label":"Person"}
headers= {'Content-Type':'application/json','Authorization':'Basic bmVvNGo6cGFzc3dvcmQ='} 
start = timer()
r = requests.post("http://54.197.127.136:7474/v1/import/cleanDB", data=json.dumps(payload),headers=headers)
print r.status_code

r.close();
end = timer()

print 'finished cleaning the database'
print(end - start)

#Load Composites
payload = {"file":"/home/ubuntu/neo4j/data/advantidata/2016_10_31_13_43_11_361733/pe_composite_nodes.csv","executionKey":"9","getCreatedDT":"11212016"}
headers= {'Content-Type':'application/json','Authorization':'Basic bmVvNGo6cGFzc3dvcmQ='} 
start = timer()
r = requests.post("http://54.197.127.136:7474/v1/import/composite", data=json.dumps(payload),headers=headers)
print r.status_code

r.close();
end = timer()

print 'finished composite nodes'
print(end - start)

#compositeRels

payload = {"file":"/home/ubuntu/neo4j/data/advantidata/2016_10_31_13_43_11_361733/pe_composite_nodes.csv","executionKey":"9","getCreatedDT":"11212016"}
headers= {'Content-Type':'application/json','Authorization':'Basic bmVvNGo6cGFzc3dvcmQ='} 
start = timer()
r = requests.post("http://54.197.127.136:7474/v1/import/compositeRels", data=json.dumps(payload),headers=headers)
print r.status_code

r.close();
end = timer()

print 'finished composite rels'
print(end - start)


#Load Issues
payload = {"file":"/home/ubuntu/neo4j/data/advantidata/2016_10_31_13_43_11_361733/pe_issue_nodes.csv","executionKey":"9","getCreatedDT":"11212016"}
headers= {'Content-Type':'application/json','Authorization':'Basic bmVvNGo6cGFzc3dvcmQ='} 
start = timer()
r = requests.post("http://54.197.127.136:7474/v1/import/issues", data=json.dumps(payload),headers=headers)
print r.status_code

r.close();
end = timer()

print 'finished issues nodes'
print(end - start)

#issue -> PE

payload = {"file":"/home/ubuntu/neo4j/data/advantidata/2016_10_31_13_43_11_361733/pe_issue_nodes.csv","executionKey":"9","getCreatedDT":"11212016"}
headers= {'Content-Type':'application/json','Authorization':'Basic bmVvNGo6cGFzc3dvcmQ='} 
start = timer()
r = requests.post("http://54.197.127.136:7474/v1/import/issueRels", data=json.dumps(payload),headers=headers)
print r.status_code

r.close();
end = timer()

print 'finished issue rels'
print(end - start)

#Load Issuer
payload = {"file":"/home/ubuntu/neo4j/data/advantidata/2016_10_31_13_43_11_361733/pe_issuer_nodes.csv","executionKey":"9","getCreatedDT":"11212016"}
headers= {'Content-Type':'application/json','Authorization':'Basic bmVvNGo6cGFzc3dvcmQ='} 
start = timer()
r = requests.post("http://54.197.127.136:7474/v1/import/issuer", data=json.dumps(payload),headers=headers)
print r.status_code

r.close();
end = timer()

print 'finished issuer nodes'
print(end - start)


#issueRels

payload = {"file":"/home/ubuntu/neo4j/data/advantidata/2016_10_31_13_57_20_261162/pe_issuer_nodes.csv","executionKey":"9","getCreatedDT":"11212016"}
headers= {'Content-Type':'application/json','Authorization':'Basic bmVvNGo6cGFzc3dvcmQ='} 
start = timer()
r = requests.post("http://54.197.127.136:7474/v1/import/issuerRels", data=json.dumps(payload),headers=headers)
print r.status_code

r.close();
end = timer()

print 'finished issuer rels'
print(end - start)



#Load IDValues
payload = {"file":"/home/ubuntu/neo4j/data/advantidata/2016_10_31_13_52_36_111597/idvalue_nodes.csv","executionKey":"9","getCreatedDT":"11212016"}
headers= {'Content-Type':'application/json','Authorization':'Basic bmVvNGo6cGFzc3dvcmQ='} 
start = timer()
r = requests.post("http://54.197.127.136:7474/v1/import/idvalues", data=json.dumps(payload),headers=headers)
print r.status_code

r.close();
end = timer()

print 'finished IDValues nodes'
print(end - start)


#idValue->PE

payload = {"file":"/home/ubuntu/neo4j/data/advantidata/2016_10_31_13_54_25_991213/pe_idvalue_rels.csv","executionKey":"9","getCreatedDT":"11212016"}
headers= {'Content-Type':'application/json','Authorization':'Basic bmVvNGo6cGFzc3dvcmQ='} 
start = timer()
r = requests.post("http://54.197.127.136:7474/v1/import/idValueRels", data=json.dumps(payload),headers=headers)
print r.status_code

r.close();
end = timer()

print 'finished idValue rels'
print(end - start)

#pe -> pe relationships

payload = {"file":"/home/ubuntu/neo4j/data/advantidata/2016_10_31_14_04_41_430894/pe_rel_nodes.csv","executionKey":"9","getCreatedDT":"11212016"}
headers= {'Content-Type':'application/json','Authorization':'Basic bmVvNGo6cGFzc3dvcmQ='} 
start = timer()
r = requests.post("http://54.197.127.136:7474/v1/import/perels", data=json.dumps(payload),headers=headers)
print r.status_code

r.close();
end = timer()

print 'finished perels rels'
print(end - start)

overallend = timer()

print 'finished the load'
print(overallend - overallstart)

