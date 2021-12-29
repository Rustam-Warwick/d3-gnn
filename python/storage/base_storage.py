from pyflink.datastream import ProcessFunction
from pyflink.datastream.functions import RuntimeContext
import re
from typing import TYPE_CHECKING, List, Iterator
from elements import ElementTypes, Op
from exceptions import NotSupported, GraphElementNotFound

if TYPE_CHECKING:
    from elements import GraphQuery, GraphElement, Rpc
    from elements.vertex import BaseVertex
    from elements.edge import BaseEdge
    from aggregator import BaseAggregator
    from elements.feature import Feature

import abc


class BaseStorage(ProcessFunction, metaclass=abc.ABCMeta):
    def __init__(self):
        super(BaseStorage, self).__init__()
        self.out: list = None
        self.part_id: int = -1
        self.aggregators: List["BaseAggregator"] = list()

    def with_aggregator(self, aggregator, *args, **kwargs) -> "BaseStorage":
        agg = aggregator(*args, storage=self, **kwargs)
        self.aggregators.append(agg)
        return self

    def open(self, runtime_context: RuntimeContext):
        self.part_id = runtime_context.get_index_of_this_subtask()
        pass

    @abc.abstractmethod
    def add_vertex(self, vertex: "BaseVertex"):
        pass

    def __add_vertex(self, vertex: "BaseVertex"):
        try:
            self.get_vertex(vertex.id)
        except GraphElementNotFound:
            self.add_vertex(vertex)
            vertex.add_storage_callback(self)
            for agg in self.aggregators: agg.add_element_callback(vertex)

    @abc.abstractmethod
    def add_edge(self, edge: "BaseEdge"):
        pass

    def __add_edge(self, edge: "BaseEdge"):
        try:
            self.get_edge(edge.id)
        except GraphElementNotFound:
            self.__add_vertex(edge.source)
            self.__add_vertex(edge.destination)
            self.add_edge(edge)
            edge.add_storage_callback(self)
            for agg in self.aggregators: agg.add_element_callback(edge)

    @abc.abstractmethod
    def update(self, element: "GraphElement"):
        """ Given already existing element re-save it since the value has changed """
        pass

    def __update(self, old_element: "GraphElement", new_element: "GraphElement"):
        """ Update request came in """
        if old_element.element_type is not new_element.element_type:
            raise NotSupported
        is_changed = old_element.update(
            new_element)  # GraphElement _sync function calls storage.update function. Because for each
        # Feature type there might be different conditions on when we need to update the storage
        if is_changed:
            self.update(old_element)
            for agg in self.aggregators: agg.update_element_callback(old_element)

    def __sync(self, old_element: "GraphElement", new_element: "GraphElement"):
        """ Update that is happening because of master sync """
        if old_element.element_type is not new_element.element_type:
            raise NotSupported
        is_changed = old_element.update(new_element)
        if is_changed:
            self.update(old_element)
            for agg in self.aggregators: agg.sync_element_callback(old_element)

    def __rpc(self, element: "GraphElement", rpc: "Rpc"):
        """ Update that is happening because of RPC message call """
        is_changed = element(rpc)
        if is_changed:
            self.update(element)

    @abc.abstractmethod
    def get_vertex(self, element_id: str) -> "BaseVertex":
        """ Return vertex of ElementNotFound Exception """
        pass

    @abc.abstractmethod
    def get_edge(self, element_id: str) -> "BaseEdge":
        """ Return Edge of ElementNotFound Exception """
        pass

    @abc.abstractmethod
    def get_incident_edges(self, vertex: "BaseVertex", n_type: str = "in") -> Iterator["BaseEdge"]:
        pass

    @abc.abstractmethod
    def get_feature(self, element_id: str) -> "Feature":
        """ Return Feature of ElementNotFound Exception """
        pass

    def get_element(self, element_id: str) -> "GraphElement":
        """ Decode the get_* functions from the id of GraphElement """
        feature_match = re.search("(?P<type>\w+):(?P<element_id>\w+):(?P<feature_name>\w+)", element_id)
        if feature_match:
            # Feature is asked
            return self.get_feature(element_id)

        edge_match = re.search("(?P<source_id>\w+):(?P<dest_id>\w+)", element_id)
        if edge_match:
            # Edge is asked
            return self.get_edge(element_id)
        # Vertex is asked
        return self.get_vertex(element_id)

    def message(self, query: "GraphQuery"):
        """ Yield message in this iteration """
        self.out.append(query)

    def process_element(self, value: "GraphQuery", ctx: 'ProcessFunction.Context'):
        self.out = list()
        try:
            if value.op is Op.RPC:
                # Exceptional case when the value is not GraphQuery! in all other cases element is a graphQuery
                el: "Rpc" = value.element
                element = self.get_element(el.id)
                if element.element_type is ElementTypes.FEATURE:
                    self.__rpc(element, el)
                else:
                    raise NotSupported
            if value.op is Op.ADD:
                el_type = value.element.element_type
                if el_type is ElementTypes.EDGE:
                    self.__add_edge(value.element)
                if el_type is ElementTypes.VERTEX:
                    self.__add_vertex(value.element)
                if el_type is ElementTypes.FEATURE:
                    raise NotSupported
            if value.op is Op.SYNC:
                el_type = value.element.element_type
                if el_type is ElementTypes.FEATURE:
                    element = self.get_feature(value.element.id)
                    self.__sync(element, value.element)
                elif el_type is ElementTypes.VERTEX:
                    element = self.get_vertex(value.element.id)
                    self.__sync(element, value.element)
                elif el_type is ElementTypes.EDGE:
                    element = self.get_edge(value.element.id)
                    self.__sync(element, value.element)
                else:
                    raise NotSupported

        except Exception as e:
            print(e.with_traceback())

        yield from self.out
