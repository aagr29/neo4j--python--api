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

#run queries
query1=session.run(q1)
query2=session.run(q2)
query3=session.run(q3)

session.close()

