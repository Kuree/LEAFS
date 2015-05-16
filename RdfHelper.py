from rdflib import Graph, Literal, BNode, Namespace, RDF, URIRef
from rdflib.namespace import DC, FOAF

def createGraph():
    graph = Graph('Sleepycat')
    graph.open('RdfStore', create = True)
    graph.close()


def testStore():
    graph = Graph('Sleepycat')
    graph.open('RdfStore')
    node = BNode()
    graph.add(node, FOAF.nick)
    pass