from pyflink.datastream import ProcessFunction
from pyflink.datastream.functions import RuntimeContext
from typing import TYPE_CHECKING
from elements import ElementTypes, Op
from exceptions import NotSupported

if TYPE_CHECKING:
    from elements import GraphQuery, GraphElement, Rpc
    from elements.vertex import BaseVertex
    from elements.edge import BaseEdge

import abc


class BaseStorage(ProcessFunction, metaclass=abc.ABCMeta):
    def __init__(self):
        super(BaseStorage, self).__init__()
        self.out: list = None
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

    @abc.abstractmethod
    def update(self, old_element: "GraphElement", new_element: "GraphElement"):
        """ Actually save element once it has changed """
        pass

    @abc.abstractmethod
    def get_element(self, element_id: str) -> "GraphElement":
        """ Return element given its id """
        pass

    def message(self, query: "GraphQuery"):
        self.out.append(query)

    def process_element(self, value: "GraphQuery", ctx: 'ProcessFunction.Context'):
        self.out = list()

        if value.op == Op.RPC:
            # Exceptional case when the value is not GraphQuery!
            el: "Rpc" = value.element
            element = self.get_element(el.id)
            if element.element_type == ElementTypes.FEATURE:
                element(el)
            else:
                raise NotSupported
        if value.op == Op.ADD:
            el_type = value.element.element_type
            if el_type == ElementTypes.EDGE:
                self.__add_edge(value.element)
            if el_type == ElementTypes.VERTEX:
                self.__add_vertex(value.element)
            if el_type == ElementTypes.FEATURE:
                raise NotSupported
        if value.op == Op.SYNC:
            print("Sync %s" % (value,))


        yield from self.out
