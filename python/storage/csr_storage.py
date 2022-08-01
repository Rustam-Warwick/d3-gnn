from typing import Literal, Iterator, Tuple

from storage import BaseStorage

class CSRStorage(BaseStorage):
    def add_feature(self, feature: "ElementFeature") -> Tuple[bool, "ElementFeature"]:
        pass

    def add_vertex(self, vertex: "BaseVertex") -> Tuple[bool, "BaseVertex"]:
        pass

    def add_edge(self, edge: "BaseEdge") -> Tuple[bool, "BaseEdge"]:
        pass

    def update_element(self, element: "GraphElement"):
        pass

    def get_vertex(self, element_id: str) -> "BaseVertex":
        pass

    def get_edge(self, element_id: str) -> "BaseEdge":
        pass

    def get_feature(self, element_id: str) -> "ElementFeature":
        pass

    def get_incident_edges(self, vertex: "BaseVertex", edge_type: Literal['in', 'out', 'both'] = "in") -> Iterator[
        "BaseEdge"]:
        pass