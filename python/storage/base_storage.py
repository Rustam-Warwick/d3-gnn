from pyflink.datastream import ProcessFunction
import re
from typing import TYPE_CHECKING, List, Iterator, Union, Tuple, Literal
from elements import ElementTypes, Op
from exceptions import NotSupported, GraphElementNotFound
from dgl import DGLGraph

if TYPE_CHECKING:
    from elements import GraphQuery, GraphElement, Rpc
    from elements.vertex import BaseVertex
    from elements.edge import BaseEdge
    from aggregator import BaseAggregator
    from elements.element_feature import ElementFeature

import abc


class BaseStorage(metaclass=abc.ABCMeta):
    """ Store is an abstraction that stores partitioned subgraph. It is usually
      attached to process function but can exist without as well. For example subgraph
      """

    @abc.abstractmethod
    def add_feature(self, feature: "ElementFeature") -> bool:
        pass

    @abc.abstractmethod
    def add_vertex(self, vertex: "BaseVertex") -> bool:
        pass

    @abc.abstractmethod
    def add_edge(self, edge: "BaseEdge") -> bool:
        pass

    @abc.abstractmethod
    def update(self, element: "GraphElement"):
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
    def get_feature(self, element_id: str) -> "ElementFeature":
        """ Return Feature of ElementNotFound Exception """
        pass

    @abc.abstractmethod
    def get_incident_edges(self, vertex: "BaseVertex", edge_type: Literal['in', 'out', 'both'] = "in") -> Iterator[
        "BaseEdge"]:
        pass

    def add_element(self, element: "GraphElement") -> bool:
        pass

    def get_element(self, element_id: str, with_features=False) -> "GraphElement":
        """ Decode the get_* functions from the id of GraphElement """
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
