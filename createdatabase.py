#initialise database connection
from neo4j import GraphDatabase
uri = "bolt://34.116.74.176:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "enzen"))
session=driver.session()

#query to create index on nodeid
q1='''CREATE INDEX nodeidindex FOR (n:Nodeid) ON (n.id)'''

#create all the nodes 
q2='''CALL apoc.periodic.iterate(" LOAD CSV WITH HEADERS FROM 'https://storage.googleapis.com/neo4j_data_aniket/build_nodes_type.csv'  
AS network return network" ,"
create (n:Nodeid{id:toInteger(network.Id),Type:network.Type})",
{batchSize:1000,iterateList:true})'''

#create all the relationships
q3='''CALL apoc.periodic.iterate(" LOAD CSV WITH HEADERS FROM 'https://storage.googleapis.com/neo4j_data_aniket/build/links.csv'  
AS network return network" ,
"
match (n:Nodeid)
where n.id=toInteger(network.Start_Node_Id) and network.End_Node_Id is not null
match (m:Nodeid)
where m.id=toInteger(network.End_Node_Id) and network.End_Node_Id is not null
create (n)-[:edgeId{edgeId: network.Id,Length:toFloat(network.Length)}]->(m)",
{batchSize:1000,iterateList:true})'''

# add sited based on nodeid
q4='''CALL apoc.periodic.iterate(" load csv with headers from 'https://storage.googleapis.com/neo4j_data_aniket/build/sitenodes.csv' as row return row ",
"match (n:Nodeid)
where n.id=toInteger(row.Node_Id) set n.Siteid=toInteger(row.Site_Id)",
{batchSize:1000,iterateList:true})'''

# create index on site id
q5=''' 
CREATE INDEX Siteidindex FOR (n:Nodeid) ON (n.Siteid)
'''

#add assest id based on site id
q6='''
CALL apoc.periodic.iterate(" load csv with headers from 'https://storage.googleapis.com/neo4j_data_aniket/build/siteassets.csv' as row return row ",
"match (n:Nodeid)
where n.Siteid=toInteger(row.Site_Id) set n.Assetid=toInteger(row.Asset_Id)",
{batchSize:1000,iterateList:true})
'''
#create index on assest id
q7='''
CREATE INDEX Assetidindex FOR (n:Nodeid) ON (n.Assetid)
'''

# add assest type based on assest id
q8='''
CALL apoc.periodic.iterate(" load csv with headers from 'https://storage.googleapis.com/neo4j_data_aniket/build/assets.csv' as row return row ",
" 
match (n:Nodeid)
where n.Assetid=toInteger(row.Id) set n.Assettype=row.Asset_Type",
{batchSize:1000,iterateList:true})
'''

#run queries
query1=session.run(q1)
print("query 1 successfull")
query2=session.run(q2)
print("query 2 successfull")
query3=session.run(q3)
print("query 3 successfull")
query4=session.run(q4)
print("query 4 successfull")
query5=session.run(q5)
print("query 5 successfull")
query6=session.run(q6)
print("query 6 successfull")
query7=session.run(q7)
print("query 7 successfull")
query8=session.run(q8)
print("query 8 successfull")
session.close()

