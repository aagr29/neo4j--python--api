from neo4j import GraphDatabase

uri = "bolt://34.116.74.176:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "enzen"))


def shortestpath(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2}))UNWIND relationships(p) AS  e return start.id,end.id, sum(e.Length) as summation", node1=node1, node2=node2)
        print(result.single())
        
print("shortest path finder between two nodes in the network")


print('Input first node:'),
a=int(input())
print('Input second node:'),
b=int(input())
with driver.session() as session:
    session.write_transaction(shortestpath,a,b)


driver.close()