from storage.linked_list_storage import LinkedListStorage
from pyflink.datastream import ProcessFunction
from exceptions import NotSupported
from elements import ElementTypes, Op
import logging
from typing import TYPE_CHECKING, List, Iterator, Union, Tuple, Literal, Dict

if TYPE_CHECKING:
    from aggregator import BaseAggregator
    from elements import GraphElement, GraphQuery
from pyflink.datastream.functions import RuntimeContext


class GraphStorageProcess(LinkedListStorage, ProcessFunction):
    def __init__(self, *args, **kwargs):
        super(GraphStorageProcess, self).__init__(*args, **kwargs)
        self.out: list = list()
        self.part_id: int = -1
        self.aggregators: Dict[str, BaseAggregator] = dict()

    def with_aggregator(self, aggregator, *args, **kwargs) -> "GraphStorageProcess":
        """ attaching aggregator to storage"""
        agg: BaseAggregator = aggregator(*args, storage=self, **kwargs)
        self.aggregators[agg.id] = agg
        return self

    def for_aggregator(self, fn):
        """ Apply a callback function for each aggregator """
        for agg in self.aggregators.values(): fn(agg)

    def open(self, runtime_context: RuntimeContext):
        """ First callback on the task process side """
        self.part_id = runtime_context.get_index_of_this_subtask()
        self.for_aggregator(lambda agg: agg.open())
        super(GraphStorageProcess, self).open(runtime_context)

    def message(self, query: "GraphQuery"):
        """ Yield message in this iteration """
        self.out.append(query)

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
                value.element.attach_storage(self)
                value.element.create_element()
            if value.op is Op.SYNC:
                el_type = value.element.element_type
                if el_type is ElementTypes.FEATURE:
                    element = self.get_feature(value.element.id)
                    element.sync_element(value.element)
                elif el_type is ElementTypes.VERTEX:
                    element = self.get_vertex(value.element.id)
                    element.sync_element(value.element)
                elif el_type is ElementTypes.EDGE:
                    element = self.get_edge(value.element.id)
                    element.sync_element(value.element)
                else:
                    raise NotSupported
            if value.op is Op.UPDATE:
                el_type = value.element.element_type
                if el_type is ElementTypes.FEATURE:
                    element = self.get_feature(value.element.id)
                    element.update_element(value.element)
                elif el_type is ElementTypes.VERTEX:
                    element = self.get_vertex(value.element.id)
                    element.update_element(value.element)
                elif el_type is ElementTypes.EDGE:
                    element = self.get_edge(value.element.id)
                    element.update_element(value.element)
                else:
                    raise NotSupported
            if value.op is Op.AGG:
                self.for_aggregator(lambda x: x.run(value))
        except Exception as e:
            print(str(e))

        while len(self.out):
            yield self.out.pop(0)
