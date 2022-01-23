from pyflink.datastream import ProcessFunction
import re
from typing import TYPE_CHECKING, List, Iterable, Union, Tuple, Literal, Dict
from elements import ElementTypes, Op
from exceptions import NotSupported, GraphElementNotFound
from dgl import DGLGraph

if TYPE_CHECKING:
    from elements import GraphQuery, GraphElement, Rpc
    from elements.vertex import BaseVertex
    from elements.edge import BaseEdge
    from aggregator import BaseAggregator
    from elements.element_feature import ReplicableFeature

import abc


class BaseStorage(metaclass=abc.ABCMeta):
    """ Store is an abstraction that stores partitioned subgraph. It is usually
      attached to process function but can exist without as well. For example subgraph
      """

    @abc.abstractmethod
    def add_feature(self, feature: "ReplicableFeature") -> bool:
        """ Add feature return if created or already existed """
        pass

    @abc.abstractmethod
    def add_vertex(self, vertex: "BaseVertex") -> bool:
        """ Add vertex return if created or already existed """
        pass

    @abc.abstractmethod
    def add_edge(self, edge: "BaseEdge") -> bool:
        """ Add edge return if created or already existed """
        pass

    @abc.abstractmethod
    def update_feature(self, feature: "ReplicableFeature") -> bool:
        pass

    @abc.abstractmethod
    def update_vertex(self, vertex: "BaseVertex") -> bool:
        pass

    @abc.abstractmethod
    def update_edge(self, vertex: "BaseVertex") -> bool:
        pass

    @abc.abstractmethod
    def get_vertex(self, element_id: str, with_features=False) -> "BaseVertex":
        """ Return vertex of ElementNotFound Exception """
        pass

    @abc.abstractmethod
    def get_edge(self, element_id: str, with_features=False) -> "BaseEdge":
        """ Return Edge of ElementNotFound Exception """
        pass

    @abc.abstractmethod
    def get_feature(self, element_id: str) -> "ReplicableFeature":
        pass

    @abc.abstractmethod
    def get_features(self, element_type: "ElementTypes", element_id:str) -> Dict[str, "ReplicableFeature"]:
        pass

    @abc.abstractmethod
    def get_incident_edges(self, vertex: "BaseVertex", edge_type: Literal['in', 'out', 'both'] = "in") -> Iterable[
        "BaseEdge"]:
        pass

    def add_element(self, element: "GraphElement") -> bool:
        """ Just a multiplexer for adding graph elements """
        if element.element_type is ElementTypes.VERTEX:
            return self.add_vertex(element)
        if element.element_type is ElementTypes.EDGE:
            return self.add_edge(element)
        if element.element_type is ElementTypes.FEATURE:
            return self.add_feature(element)

    def get_element(self, element_id: str, with_features=False) -> "GraphElement":
        """ Just a simple MUX for getting graph elements """
        feature_match = re.search("(?P<type>\w+):(?P<element_id>\w+):(?P<feature_name>\w+)", element_id)
        if feature_match:
            # Feature is asked
            return self.get_feature(element_id)

        edge_match = re.search("(?P<source_id>\w+):(?P<dest_id>\w+)", element_id)
        if edge_match:
            # Edge is asked
            return self.get_edge(element_id, with_features)
        # Vertex is asked
        return self.get_vertex(element_id, with_features)

    def update_element(self, element: "GraphElement"):
        if element.element_type is ElementTypes.FEATURE:
            return self.update_feature(element)
        if element.element_type is ElementTypes.VERTEX:
            return self.update_vertex(element)
        if element.element_type is ElementTypes.EDGE:
            return self.update_edge(element)