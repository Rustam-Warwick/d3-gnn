from storage.linked_list_storage import LinkedListStorage
from pyflink.datastream import ProcessFunction
from copy import copy
from traceback import print_exc
from exceptions import *
from elements import ElementTypes, Op, IterationState
from typing import TYPE_CHECKING, Dict
from elements import GraphQuery

if TYPE_CHECKING:
    from aggregator import BaseAggregator
from pyflink.datastream.functions import RuntimeContext


class GNNLayerProcess(LinkedListStorage, ProcessFunction):
    def __init__(self, *args, position=1, layers=1, **kwargs):
        super(GNNLayerProcess, self).__init__(*args, **kwargs)
        self.out: list = list()  # List storing the message to be sent
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
            agg_init(agg)
        del self.plugins
        super(GNNLayerProcess, self).open(runtime_context)

    def message(self, query: "GraphQuery"):
        """ Yield message in this iteration """
        self.out.append(query)

    def debug_print(self, *vertex_ids):
        def fn():
            for i in vertex_ids:
                try:
                    salam = self.get_vertex(i)
                    print(salam['feature'].value, self.part_id, salam.state, salam.integer_clock)
                except GraphElementNotFound:
                    pass
                except KeyError:
                    pass

        fn()

    def process_element(self, value: "GraphQuery", ctx: 'ProcessFunction.Context'):
        if value.is_topology_change and not self.is_last:
            # Redirect to the next operator.
            # Should be here so that subsequent layers have received updated topology state before any other thing
            yield value
        # if self.is_first:
        #     self.debug_print("015889")
        try:
            if value.op is Op.RPC:
                # Exceptional case when the value is not GraphQuery! in all other cases element is a graphQuery
                el: "Rpc" = value.element
                element = self.get_element_by_id(el.id)
                element(el)
            if value.op is Op.ADD:
                # because there might be late events it is always good to create element first
                value.element.attach_storage(self)
                value.element.create_element()
            if value.op is Op.SYNC:
                el = self.get_element(value.element, False)
                if el is None:
                    el = copy(value.element)  # This copy is needed, so we don't lose the part_id after attach_storage
                    el.attach_storage(self)
                    el.create_element()
                    el = self.get_element(value.element, False)
                el.sync_element(value.element)

            if value.op is Op.ADDUPDATE:
                # @todo This is never later because updates get distributed in partitioning stage
                el = self.get_element(value.element)
                el.external_update(value.element)
            if value.op is Op.AGG:
                self.get_aggregator(value.aggregator_name).run(value)
        except GraphElementNotFound:
            print("Graph Element Not Found Exception", value.op)
        except NotSupported:
            print("We do not support such message type")
        except CreateElementFailException:
            print("Failed to create element exception")
        except Exception as e:
            print_exc()
        while len(self.out):
            yield self.out.pop(0)
