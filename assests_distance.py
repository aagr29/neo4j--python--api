from neo4j import GraphDatabase

uri = "bolt://34.116.74.176:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "enzen"))


def shortestpath(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2}))UNWIND relationships(p) AS  e return start.id,end.id, sum(e.Length) as summation", node1=node1, node2=node2)
        print(result.single())

print("shortest path finder between two nodes in the network")
#pole count
def polescount(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2})) unwind nodes(p) as e with e.Assettype as e where e.Assettype='Pole' return count(e) as poles", node1=node1, node2=node2)
        print(result.single())

def Cubiclescount(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2})) unwind nodes(p) as e with e.Assettype as e where e.Assettype='Cubicle' return count(e) as cubicles", node1=node1, node2=node2)
        print(result.single())

def Substationcount(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2})) unwind nodes(p) as e with e.Assettype as e where e.Assettype='Substation' return count(e) as Substations", node1=node1, node2=node2)
        print(result.single())

def Premisecount(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2})) unwind nodes(p) as e with e.Assettype as e where e.Assettype='Premise' return count(e) as Premises", node1=node1, node2=node2)
        print(result.single())

def Switchcount(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2})) unwind nodes(p) as e with e.Assettype as e where e.Assettype='Switch' return count(e) as Switchs", node1=node1, node2=node2)
        print(result.single())

def Regulator_Sit_count(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2})) unwind nodes(p) as e with e.Assettype as e where e.Assettype='Regulator Site' return count(e) as Regulators", node1=node1, node2=node2)
        print(result.single())

def Metering_Unit_count(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2})) unwind nodes(p) as e with e.Assettype as e where e.Assettype='Metering Unit' return count(e) as Metering_Units", node1=node1, node2=node2)
        print(result.single())
print('Input first node:'),
a=int(input())
print('Input second node:'),
b=int(input())
with driver.session() as session:
    session.write_transaction(shortestpath,a,b)
    session.write_transaction(polescount,a,b)
    session.write_transaction(Cubiclescount,a,b)
    session.write_transaction(Substationcount,a,b)
    session.write_transaction(Premisecount,a,b)
    session.write_transaction(Switchcount,a,b)
    session.write_transaction(Regulator_Sit_count,a,b)
    session.write_transaction(Metering_Unit_count,a,b)


driver.close()