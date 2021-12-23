from elements.edge import BaseEdge
from elements.vertex import BaseVertex
from storage import BaseStorage


class HashMapStorage(BaseStorage):
    """Simple in-memory storage that stores everything in a hash-table"""
    def __init__(self):
        super(HashMapStorage, self).__init__()
        self.vertices = dict()
        self.edges = dict()

    def add_vertex(self, vertex: BaseVertex):
        self.vertices[vertex.id] = vertex

    def add_edge(self, edge: BaseEdge):
        self.edges[edge.id] = edge


