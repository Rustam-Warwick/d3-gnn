from storage.map_storage import HashMapStorage
from pyflink.datastream import ProcessFunction
from exceptions import NotSupported
from elements import ElementTypes, Op
import logging
from typing import TYPE_CHECKING, List, Iterator, Union, Tuple, Literal, Dict

if TYPE_CHECKING:
    from aggregator import BaseAggregator
from pyflink.datastream.functions import RuntimeContext


class GraphStorageProcess(HashMapStorage, ProcessFunction):
    def __init__(self, *args, **kwargs):
        super(GraphStorageProcess, self).__init__(*args, **kwargs)
        self.out: list = list()
        self.part_id: int = -1
        self.aggregators: Dict[str, BaseAggregator] = dict()

    def with_aggregator(self, aggregator, *args, **kwargs) -> "GraphStorageProcess":
        agg: BaseAggregator = aggregator(*args, storage=self, **kwargs)
        self.aggregators[agg.id] = agg
        return self

    def for_aggregator(self, fn):
        for agg in self.aggregators: fn(agg)

    def open(self, runtime_context: RuntimeContext):
        self.part_id = runtime_context.get_index_of_this_subtask()
        self.for_aggregator(lambda agg: agg.open())
        super(GraphStorageProcess, self).open(runtime_context)

    def message(self, query: "GraphQuery"):
        """ Yield message in this iteration """
        self.out.append(query)

    def _add_feature(self, feature: "Feature") -> "Feature":
        created, feature = self.add_feature(feature)
        if created:
            feature.add_storage_callback(self)
            self.for_aggregator(lambda x: x.add_element_callback(feature))
        return feature

    def _add_vertex(self, vertex: "BaseVertex") -> "BaseVertex":
        for name, feature in vertex.features:
            feature = self._add_feature(feature)

        created, vertex = self.add_vertex(vertex)
        if created:
            vertex.add_storage_callback(self)
            self.for_aggregator(lambda x: x.add_element_callback(vertex))
        return vertex

    def _add_edge(self, edge: "BaseEdge") -> "BaseEdge":
        for name, feature in edge.features:
            feature = self._add_feature(feature)

        edge.source = self._add_vertex(edge.source)
        edge.destination = self._add_vertex(edge.destination)
        created, edge = self.add_edge(edge)
        if created:
            edge.add_storage_callback(self)
            self.for_aggregator(lambda x: x.add_element_callback(edge))
        return edge

    def process_element(self, value: "GraphQuery", ctx: 'ProcessFunction.Context'):
        if value.is_topology_change:
            # Redirect to the next operator
            yield value
        try:
            if value.op is Op.RPC:
                # Exceptional case when the value is not GraphQuery! in all other cases element is a graphQuery
                el: "Rpc" = value.element
                element = self.get_element(el.id)
                if element.element_type is ElementTypes.FEATURE:
                    element(el)
                else:
                    raise NotSupported
            if value.op is Op.ADD:
                el_type = value.element.element_type
                if el_type is ElementTypes.EDGE:
                    self._add_edge(value.element)
                if el_type is ElementTypes.VERTEX:
                    self._add_vertex(value.element)
                if el_type is ElementTypes.FEATURE:
                    raise NotSupported
            if value.op is Op.SYNC:
                el_type = value.element.element_type
                if el_type is ElementTypes.FEATURE:
                    element = self.get_feature(value.element.id)
                    element._sync(value.element)
                elif el_type is ElementTypes.VERTEX:
                    element = self.get_vertex(value.element.id)
                    element._sync(value.element)
                elif el_type is ElementTypes.EDGE:
                    element = self.get_edge(value.element.id)
                    element._sync(value.element)
                else:
                    raise NotSupported
            if value.op is Op.UPDATE:
                el_type = value.element.element_type
                if el_type is ElementTypes.FEATURE:
                    element = self.get_feature(value.element.id)
                    element._update(value.element)
                elif el_type is ElementTypes.VERTEX:
                    element = self.get_vertex(value.element.id)
                    element._update(value.element)
                elif el_type is ElementTypes.EDGE:
                    element = self.get_edge(value.element.id)
                    element._update(value.element)
                else:
                    raise NotSupported
            if value.op is Op.AGG:
                self.for_aggregator(lambda x: x.run(value))
        except Exception as e:
            logging.log(logging.ERROR, e)

        while len(self.out):
            yield self.out.pop(0)
