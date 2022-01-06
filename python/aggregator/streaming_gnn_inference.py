import abc

from aggregator import BaseAggregator
from elements import ElementTypes, GraphQuery, Op
from typing import TYPE_CHECKING
from copy import copy
import torch
from abc import ABCMeta

if TYPE_CHECKING:
    from elements.edge import SimpleEdge
    from elements.feature import Feature
    from elements.vertex import SimpleVertex
    from elements import GraphElement


class BaseStreamingGNNInference(BaseAggregator, metaclass=ABCMeta):

    def __init__(self, ident: str = "streaming_gnn", storage: "BaseStorage" = None):
        super(BaseStreamingGNNInference, self).__init__(ident, storage)
        self.queue = dict()

    @abc.abstractmethod
    def exchange(self, edge: "SimpleEdge"):
        pass

    @abc.abstractmethod
    def apply(self, vertex: "SimpleVertex") -> "torch.tensor":
        pass

    def run(self, *args, **kwargs):
        pass

    def add_element_callback(self, element: "GraphElement"):
        if element.element_type is ElementTypes.EDGE:
            if element.source.is_ready and element.destination.is_ready:
                self.exchange(element)
            else:
                self.queue[element.source.id] = element.destination.id

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        pass

    def sync_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        if element.element_type is ElementTypes.VERTEX:
            for src, dest in self.queue.items():
                if src == element.id:
                    destination = self.storage.get_vertex(dest)
                    if destination.is_ready:
                        edge = self.storage.get_edge(element.id + ":" + destination.id)
                        self.exchange(edge)
                if dest == element.id:
                    source = self.storage.get_vertex(src)
                    if source.is_ready:
                        edge = self.storage.get_edge(source.id + ":" + element.id)
                        self.exchange(edge)

    def rpc_element_callback(self, element: "GraphElement"):
        if element.element_type is ElementTypes.FEATURE:
            tmp: "Feature" = element
            if tmp.field_name == "agg":
                res = self.apply(tmp.element)
                tmp_new = copy(tmp)
                tmp_new._value = res
                query = GraphQuery(Op.UPDATE, tmp_new, self.storage.part_id, False)
                self.storage.message(query)


class StreaminGNNInference(BaseStreamingGNNInference):
    def __init__(self, *args, **kwargs):
        super(StreaminGNNInference, self).__init__(*args, **kwargs)
        self.model = None
        self.model2 = None

    def exchange(self, edge: "SimpleEdge"):
        source: "SimpleVertex" = edge.source
        dest: "SimpleVertex" = edge.destination
        with torch.no_grad():
            conc = torch.concat((source.image.value, dest.image.value), dim=1)
            msg = self.model(conc)
            dest.agg.add(msg)

    def apply(self, vertex: "SimpleVertex") -> torch.tensor:
        with torch.no_grad():
            conc = torch.concat((vertex.image.value, vertex.agg.value), dim=1)
            return self.model2(conc)

    def open(self, *args, **kwargs):
        self.model = torch.nn.Linear(32, 16, dtype=torch.float32)
        self.model2 = torch.nn.Sequential(
            torch.nn.Linear(32, 128),
            torch.nn.ReLU(),
            torch.nn.Linear(128, 32),
            torch.nn.ReLU(),
            torch.nn.Linear(32, 16)
        )
