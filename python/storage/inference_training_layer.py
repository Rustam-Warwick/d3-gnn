from storage.linked_list_storage import LinkedListStorage
from pyflink.datastream import CoProcessFunction
from exceptions import NotSupported, AggregatorExistsException
from elements import ElementTypes, Op
from typing import TYPE_CHECKING, List, Iterator, Union, Tuple, Literal, Dict
from elements import GraphQuery
if TYPE_CHECKING:
    from aggregator import BaseAggregator
    from elements.element_feature import ReplicableFeature
from pyflink.datastream.functions import RuntimeContext


class InferenceTrainingLayer(LinkedListStorage, CoProcessFunction):
    def __init__(self, *args, is_last=False, **kwargs):
        super(InferenceTrainingLayer, self).__init__(*args, **kwargs)
        self.out: list = list()  # List storing the message to be sent
        self.part_id: int = -1  # Index of this parallel task
        self.aggregators: Dict[str, BaseAggregator] = dict()  # Dict of aggregators attached
        self.is_last = is_last  # Is this GraphStorageProcess the last one in the pipeline
        self.training_samples = set()

    def with_aggregator(self, agg: "BaseAggregator") -> "GNNLayerProcess":
        """ Attaching aggregator to this process function """
        agg.storage = self
        if agg.id in self.aggregators:
            raise AggregatorExistsException
        self.aggregators[agg.id] = agg
        return self

    def for_aggregator(self, fn):
        """ Apply a callback function for each aggregator """
        for agg in self.aggregators.values(): fn(agg)

    def open(self, runtime_context: RuntimeContext):
        """ First callback on the task process side """
        self.part_id = runtime_context.get_index_of_this_subtask()
        self.for_aggregator(lambda agg: agg.open())
        super(InferenceTrainingLayer, self).open(runtime_context)

    def message(self, query: "GraphQuery"):
        """ Yield message in this iteration """
        self.out.append(query)

    def process_element1(self, value: "GraphQuery", ctx: 'CoProcessFunction.Context'):
        """ Main Stream that comes from the previous layers """
        if value.is_topology_change and not self.is_last:
            # Redirect to the next operator.
            # Should be here so that subsequent layers have received updated topology state before any other thing
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
                self.aggregators[value.aggregator_name].run(value)
        except Exception as e:
            print("ERROR")
            print(str(e))

        while len(self.out):
            yield self.out.pop(0)