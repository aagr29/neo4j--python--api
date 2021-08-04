import numpy as np
import pandas as pd 
df1 = pd.read_csv("https://storage.googleapis.com/neo4j_data_aniket/parentchild.csv")
nodeid = df1['startVertex'].tolist()
nodeid1 = df1['childVertex'].tolist()
df = pd.DataFrame(list(zip(nodeid, nodeid1)),
               columns =['Startnode', 'Endnode'])
l1=[]
l2=[]
l3=[]
l4=[]
l5=[]
l6=[]
l7=[]
l8=[]
i1=[]
i2=[]


from neo4j import GraphDatabase
count=len(nodeid1)
uri = "bolt://34.116.74.176:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "enzen"))


def shortestpath(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2}))UNWIND relationships(p) AS  e return start.id,end.id, sum(e.Length) as summation", node1=node1, node2=node2)
        a1=result.single()[2]
        return a1

print("shortest path finder between two nodes in the network")

#pole count
def polescount(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2})) unwind nodes(p) as e with e.Assettype as e where e.Assettype='Pole' return count(e) as poles", node1=node1, node2=node2)
        a2=result.single()[0]
        return a2

def Cubiclescount(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2})) unwind nodes(p) as e with e.Assettype as e where e.Assettype='Cubicle' return count(e) as cubicles", node1=node1, node2=node2)
        a3=result.single()[0]
        return a3

def Substationcount(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2})) unwind nodes(p) as e with e.Assettype as e where e.Assettype='Substation' return count(e) as Substations", node1=node1, node2=node2)
        a4=result.single()[0]
        return a4

def Premisecount(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2})) unwind nodes(p) as e with e.Assettype as e where e.Assettype='Premise' return count(e) as Premises", node1=node1, node2=node2)
        a5=result.single()[0]
        return a5

def Switchcount(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2})) unwind nodes(p) as e with e.Assettype as e where e.Assettype='Switch' return count(e) as Switchs", node1=node1, node2=node2)
        a6=result.single()[0]
        return a6

def Regulator_Sit_count(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2})) unwind nodes(p) as e with e.Assettype as e where e.Assettype='Regulator Site' return count(e) as Regulators_count", node1=node1, node2=node2)
        a7=result.single()[0]
        return a7

def Metering_Unit_count(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2})) unwind nodes(p) as e with e.Assettype as e where e.Assettype='Metering Unit' return count(e) as Metering_Units", node1=node1, node2=node2)
        a8=result.single()[0]
        return a8

for ind in df.index:
    a=int(df['Startnode'][ind])
    b=int(df['Endnode'][ind])
    with driver.session() as session:
        q1=session.write_transaction(shortestpath,a,b)
        q2=session.write_transaction(polescount,a,b)
        q3=session.write_transaction(Cubiclescount,a,b)
        q4=session.write_transaction(Substationcount,a,b)
        q5=session.write_transaction(Premisecount,a,b)
        q6=session.write_transaction(Switchcount,a,b)
        q7=session.write_transaction(Regulator_Sit_count,a,b)
        q8=session.write_transaction(Metering_Unit_count,a,b)
    i1.append(a)
    i2.append(b)
    l1.append(q1)
    l2.append(q2)
    l3.append(q3)
    l4.append(q4)
    l5.append(q5)
    l6.append(q6)
    l7.append(q7)
    l8.append(q8)
    count=count-1
    print(count)
driver.close()
print("end")

df2 = pd.DataFrame(list(zip(i1,i2,l1, l2,l3,l4,l5,l6,l7,l8)),
               columns =['Startnode','Endnode','Nodes/Length', 'polescount','Cubiclescount','Substationcount','Premisecount','Switchcount','Regulator_Site_count','Metering_Unit_count'])
df2.to_csv('datadrame_full.csv', index=False)
