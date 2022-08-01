from storage.linked_list_storage import LinkedListStorage
from pyflink.datastream import ProcessFunction, KeyedProcessFunction
from copy import copy
from traceback import print_exc
from exceptions import *
from elements import ElementTypes, Op, IterationState
from typing import TYPE_CHECKING, Dict
from elements import GraphQuery

if TYPE_CHECKING:
    from aggregator import BaseAggregator
from pyflink.datastream.functions import RuntimeContext


class KeyedGNNLayerProcess(LinkedListStorage, KeyedProcessFunction):
    def __init__(self, *args, position=1, layers=1, **kwargs):
        super(KeyedGNNLayerProcess, self).__init__(*args, **kwargs)
        self.out: list = list()  # List storing the message to be sent
        self.last_watermark = 0
        self.part_version = 1
        self.part_id: int = -1  # Index of this parallel task
        self.parallelism: int = 0
        self.plugins: Dict[str, BaseAggregator] = dict()  # Dict of plugins attached
        self.position = position  # Is this GraphStorageProcess the last one in the pipeline
        self.layers = layers  # Is this GraphStorageProcess the last one in the pipeline

    @property
    def is_last(self):
        return self.position >= self.layers

    @property
    def is_first(self):
        return self.position == 1

    def with_aggregator(self, agg: "BaseAggregator") -> "GNNLayerProcess":
        """ Attaching aggregator to this process function """
        if agg.id in self.plugins:
            raise AggregatorExistsException
        self.plugins[agg.id] = agg
        return self

    def for_aggregator(self, fn):
        """ Apply a callback function for each aggregator """
        for agg in self.get_aggregators(): fn(agg)

    def open(self, runtime_context: RuntimeContext):
        """ First callback on the task process side """

        def agg_init(agg):
            agg.attach_storage(self)
            agg.create_element()

        self.part_id = runtime_context.get_index_of_this_subtask()
        self.parallelism = runtime_context.get_number_of_parallel_subtasks()
        for agg in self.plugins.values():
            agg.open(runtime_context)
            agg_init(agg)
        del self.plugins
        super(KeyedGNNLayerProcess, self).open(runtime_context)

    def message(self, query: "GraphQuery"):
        """ Yield message in this iteration """
        self.out.append(query)

    def custom_on_watermark(self):
        for agg in self.get_aggregators(): agg.on_watermark()

    def process_element(self, value: "GraphQuery", ctx: 'KeyedProcessFunction.Context'):
        # if ctx.timer_service().current_watermark() > self.last_watermark:
        #     self.last_watermark = ctx.timer_service().current_watermark()
        #     self.custom_on_watermark()
        self.part_id = ctx.get_current_key()
        try:
            if value.op is Op.RPC:
                # Exceptional case when the value is not GraphQuery! in all other cases element is a graphQuery
                el: "Rpc" = value.element
                element = self.get_element_by_id(el.id)
                element(el)
            if value.op is Op.SYNC:
                el = self.get_element(value.element, False)
                if el is None:
                    if not self.is_last and (value.element.element_type is ElementTypes.EDGE or value.element.element_type is ElementTypes.VERTEX):
                        next_query = GraphQuery(Op.ADDUPDATE, copy(value.element), value.part, IterationState.FORWARD)
                        yield next_query
                    el = copy(value.element)  # This copy is needed, so we don't lose the part_id after attach_storage
                    el.attach_storage(self)
                    el.create_element()
                    el = self.get_element(value.element)
                el.sync_element(value.element)
            if value.op is Op.ADDUPDATE:
                el = self.get_element(value.element, False)
                if el is None:
                    if not self.is_last and (value.element.element_type is ElementTypes.EDGE or value.element.element_type is ElementTypes.VERTEX):
                        next_query = GraphQuery(Op.ADDUPDATE, copy(value.element), value.part, IterationState.FORWARD)
                        yield next_query
                    value.element.attach_storage(self)
                    value.element.create_element()
                else:
                    el.external_update(value.element)
            if value.op is Op.AGG:
                self.get_aggregator(value.aggregator_name).run(value)
        except GraphElementNotFound:
            print("Graph Element Not Found Exception", value.op, value.element.id, value.element.fn_name)
        except NotSupported:
            print("We do not support such message type")
        except CreateElementFailException:
            print("Failed to create element exception")
        except Exception as e:
            print_exc()
        yield from self.out
        self.out.clear()
