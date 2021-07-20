#initialise database connection
from neo4j import GraphDatabase
uri = "bolt://34.116.74.176:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "enzen"))
session=driver.session()
#query to delete nodes and relationships
q1='''call apoc.periodic.iterate("MATCH (n) return n", "DETACH DELETE n", {batchSize:1000})
yield batches, total return batches, total'''

#query to drop index
q2='''drop index nodeidindex'''

#run queries
query1=session.run(q1)
query2=session.run(q2)
session.close()