from pyflink.datastream import ProcessFunction
from pyflink.datastream.functions import RuntimeContext
from typing import TYPE_CHECKING
from elements import ElementTypes, Op

if TYPE_CHECKING:
    from elements import GraphQuery, GraphElement
    from elements.vertex import BaseVertex
    from elements.edge import BaseEdge

import abc


class BaseStorage(ProcessFunction, metaclass=abc.ABCMeta):
    def __init__(self):
        super(BaseStorage, self).__init__()
        self.part_id: int = -1

    def open(self, runtime_context: RuntimeContext):
        self.part_id = runtime_context.get_index_of_this_subtask()
        pass

    @abc.abstractmethod
    def add_vertex(self, vertex: "BaseVertex"):
        pass

    def __add_vertex(self, vertex: "BaseVertex"):
        vertex.pre_add_storage_callback(self)
        self.add_vertex(vertex)
        vertex.post_add_storage_callback(self)

    @abc.abstractmethod
    def add_edge(self, edge: "BaseEdge"):
        pass

    def __add_edge(self, edge: "BaseEdge"):
        edge.pre_add_storage_callback(self)
        self.add_edge(edge)
        edge.post_add_storage_callback(self)

    def msg_replicas(self, el: "GraphElement"):
        print("Asking for msg")
        pass

    def msg_master(self, el: "GraphElement"):
        print("Asking for Master msg")
        pass

    def process_element(self, value: "GraphQuery", ctx: 'ProcessFunction.Context'):
        el_type = value.element.element_type
        if value.op == Op.ADD:
            if el_type == ElementTypes.EDGE:
                self.__add_edge(value.element)
            if el_type == ElementTypes.VERTEX:
                self.__add_vertex(value.element)
