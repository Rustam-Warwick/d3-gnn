from typing import List, Iterator, Literal, Dict

from elements.edge import BaseEdge
from elements.vertex import BaseVertex
from elements.element_feature import ElementFeature
from storage import BaseStorage
from elements.graph_element import ElementTypes
from exceptions import GraphElementNotFound, NotSupported
import re


class HashMapStorage(BaseStorage):
    """Simple in-memory storage that stores everything in a hash-table"""

    def __init__(self):
        super(HashMapStorage, self).__init__()
        self.vertices: Dict["BaseVertex"] = dict()
        self.features: Dict["ElementFeature"] = dict()
        self.edges: Dict["BaseEdge"] = dict()

    def add_vertex(self, vertex: BaseVertex) -> bool:
        if vertex.id not in self.vertices:
            self.vertices[vertex.id] = vertex
            return True
        return False

    def add_feature(self, feature: "Feature") -> bool:
        return True

    def add_edge(self, edge: BaseEdge) -> bool:
        if edge.id not in self.edges:
            self.edges[edge.id] = edge
            return True
        return False

    def get_vertex(self, element_id: str, with_features=False) -> "BaseVertex":
        if element_id in self.vertices:
            return self.vertices[element_id]
        raise GraphElementNotFound

    def get_incident_edges(self, vertex: "BaseVertex", n_type: Literal['in', 'out', 'both'] = "in") -> Iterator["BaseEdge"]:
        if n_type == "in":
            return filter(lambda x: x.destination == vertex, self.edges.values())
        elif n_type == "out":
            return filter(lambda x: x.source == vertex, self.edges.values())
        elif n_type == 'both':
            return filter(lambda x: x.source == vertex or x.destination == vertex, self.edges.values())
        else:
            raise NotSupported

    def get_edge(self, element_id: str, with_features = False) -> "BaseEdge":
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
            # This is edge element_feature
            edge = self.get_edge(e_id)
            return getattr(edge, f_name)
        elif e_type == ElementTypes.VERTEX.value:
            #  This is vertex element_feature
            vertex = self.get_vertex(e_id)
            return getattr(vertex, f_name)
        raise GraphElementNotFound

    def update(self, element: "GraphElement"):
        super(HashMapStorage, self).update(element)
        pass
