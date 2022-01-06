from typing import List,Iterator

from elements.edge import BaseEdge
from elements.vertex import BaseVertex
from storage import BaseStorage
from elements.graph_element import ElementTypes
from exceptions import GraphElementNotFound, NotSupported
import re


class HashMapStorage(BaseStorage):
    """Simple in-memory storage that stores everything in a hash-table"""

    def __init__(self):
        super(HashMapStorage, self).__init__()
        self.vertices: List["BaseVertex"] = dict()
        self.edges: List["BaseEdge"] = dict()

    def add_vertex(self, vertex: BaseVertex):
        if vertex.id not in self.vertices:
            self.vertices[vertex.id] = vertex
            return True, vertex
        return False, self.vertices[vertex.id]

    def add_feature(self, feature: "Feature") -> bool:
        return (True, feature)

    def add_edge(self, edge: BaseEdge):
        if edge.id not in self.edges:
            self.edges[edge.id] = edge
            return True, edge
        return False, self.edges[edge.id]

    def get_vertex(self, element_id: str) -> "BaseVertex":
        if element_id in self.vertices:
            return self.vertices[element_id]
        raise GraphElementNotFound

    def get_incident_edges(self, vertex: "BaseVertex", n_type: str = "in") -> Iterator["BaseEdge"]:
        if n_type == "in":
            return filter(lambda x: x.destination == vertex, self.edges)
        elif n_type == "out":
            return filter(lambda x: x.source == vertex, self.edges)
        else:
            raise NotSupported

    def get_edge(self, element_id: str) -> "BaseEdge":
        if element_id in self.edges:
            return self.edges[element_id]
        raise GraphElementNotFound

    def get_feature(self, element_id: str) -> "Feature":
        feature_match = re.search("(?P<type>\w+):(?P<element_id>\w+):(?P<feature_name>\w+)", element_id)
        if not feature_match:
            raise GraphElementNotFound
        e_type = int(feature_match.group("type"))
        e_id = feature_match.group("element_id")
        f_name = feature_match.group("feature_name")
        if e_type == ElementTypes.EDGE.value:
            # This is edge feature
            edge = self.get_edge(e_id)
            return getattr(edge, f_name)
        elif e_type == ElementTypes.VERTEX.value:
            #  This is vertex feature
            vertex = self.get_vertex(e_id)
            return getattr(vertex, f_name)
        raise GraphElementNotFound

    def update(self, element: "GraphElement"):
        element.integer_clock += 1
        element.sync_replicas()
        pass
