from elements.edge import BaseEdge
from elements.vertex import BaseVertex
from storage import BaseStorage
import re


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

    def get_element(self, element_id: str) -> "GraphElement":
        feature_match = re.search("(?P<type>\w+):(?P<element_id>\w+):(?P<feature_name>\w+)", element_id)
        if feature_match:
            # Feature is asked
            pass
        edge_match = re.search("(?P<source_id>\w+):(?P<dest_id>\w+)", element_id)
        if edge_match:
            # Edge is asked
            pass
        # Vertex is asked

    def update(self, old_element: "GraphElement", new_element: "GraphElement"):
        pass
