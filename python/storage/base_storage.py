import re
from typing import TYPE_CHECKING, Iterator, Literal, Dict
from elements import ElementTypes, Op
from exceptions import NotSupported, GraphElementNotFound

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
    def add_aggregator(self, agg: "BaseAggregator") -> bool:
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
    def get_vertex(self, element_id: str, with_features: bool = False) -> "BaseVertex":
        """ Return vertex of ElementNotFound Exception """
        pass

    @abc.abstractmethod
    def get_vertices(self) -> Iterator["BaseVertex"]:
        """ Return Iterator over all vertices """
        pass

    @abc.abstractmethod
    def get_edge(self, element_id: str, with_features: bool = False) -> "BaseEdge":
        """ Return Edge of ElementNotFound Exception """
        pass

    @abc.abstractmethod
    def get_feature(self, element_id: str, with_element: bool = True) -> "ReplicableFeature":
        """ Returns single Feature from Feature id """
        pass

    @abc.abstractmethod
    def get_aggregator(self, element_id: str, with_features: bool = False) -> "BaseAggregator":
        """ Add edge return if created or already existed """
        pass

    @abc.abstractmethod
    def get_aggregators(self, with_features: bool = False) -> Iterator["BaseAggregator"]:
        """ Add edge return if created or already existed """
        pass

    @abc.abstractmethod
    def get_features(self, element_type: "ElementTypes", element_id: str) -> Dict[str, "ReplicableFeature"]:
        """ Returns Features belonging to some element type """
        pass

    @abc.abstractmethod
    def get_incident_edges(self, vertex: "BaseVertex", edge_type: Literal['in', 'out', 'both'] = "in") -> Iterator[
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
        if element.element_type is ElementTypes.AGG:
            return self.add_aggregator(element)

    def get_element(self, element: "GraphElement", throws_exception=True) -> "GraphElement":
        """ Get updated element by giving an instance of element """
        try:
            if element.element_type is ElementTypes.VERTEX:
                return self.get_vertex(element.id)

            if element.element_type is ElementTypes.EDGE:
                return self.get_edge(element.id)

            if element.element_type is ElementTypes.FEATURE:
                return self.get_feature(element.id)

            if element.element_type is ElementTypes.AGG:
                return self.get_aggregator(element.id)
        except GraphElementNotFound:
            if throws_exception:
                raise GraphElementNotFound
            else:
                return None

    def get_element_by_id(self, element_id: str, with_features=False) -> "GraphElement":
        """ Just a simple MUX for getting graph elements """
        element_type = int(element_id[0])
        if element_type is ElementTypes.FEATURE.value:
            return self.get_feature(element_id)
        if element_type is ElementTypes.EDGE.value:
            return self.get_edge(element_id, with_features)
        if element_type is ElementTypes.VERTEX.value:
            return self.get_vertex(element_id, with_features)
        if element_type is ElementTypes.AGG.value:
            return self.get_aggregator(element_id, with_features)

    def update_element(self, element: "GraphElement"):
        if element.element_type is ElementTypes.FEATURE:
            return self.update_feature(element)
        if element.element_type is ElementTypes.VERTEX:
            return self.update_vertex(element)
        if element.element_type is ElementTypes.EDGE:
            return self.update_edge(element)
