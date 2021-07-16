from neo4j import GraphDatabase

class HelloWorldExample:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_greeting(self, node1, node2):
        with self.driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, node1, node2)
            print(greeting)

    @staticmethod
    def _create_and_return_greeting(tx, node1, node2):
        result = tx.run("MATCH p=shortestpath((start:Nodeid{id:$node1})-[:edgeId*]- (end:Nodeid{id:$node2}))UNWIND relationships(p) AS  e return start.id,end.id, sum(e.Length) as summation", node1=node1, node2=node2)
        return result.single()[2]



if __name__ == "__main__":
    a=int(input())
    b=int(input())
    greeter = HelloWorldExample("bolt://34.116.74.176:7687", "neo4j", "enzen")
    greeter.print_greeting(a, b)
    greeter.close()