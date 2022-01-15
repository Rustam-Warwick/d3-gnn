import abc

from aggregator import BaseAggregator
from elements import ElementTypes, GraphQuery, Op
from typing import TYPE_CHECKING
from copy import copy
import torch
from abc import ABCMeta

if TYPE_CHECKING:
    from elements.edge import SimpleEdge
    from elements.element_feature import ReplicableFeature
    from elements.vertex import SimpleVertex
    from elements import GraphElement


class BaseStreamingGNNInference(BaseAggregator, metaclass=ABCMeta):

    def __init__(self,  minLevel=0, maxLevel=2,  ident: str = "streaming_gnn", storage: "BaseStorage" = None):
        super(BaseStreamingGNNInference, self).__init__(ident, storage)
        self.minLevel = minLevel
        self.maxLevel = maxLevel
    """ Abstract Functions """
    @abc.abstractmethod
    def exchange(self, edge: "SimpleEdge"):
        """ Enumerate each edge Increment the aggregate function """
        pass

    @abc.abstractmethod
    def apply(self, vertex: "SimpleVertex") -> "torch.tensor":
        pass

    """ Override Functions """
    def run(self, *args, **kwargs):
        pass

    def add_element_callback(self, element: "GraphElement"):
        if element.element_type is ElementTypes.EDGE:
            element: "SimpleEdge"
            if element.source.is_initialized and element.destination.is_initialized:
                # If both are initialized
                self.exchange(element)  # Update the agg function

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        if element.element_type is ElementTypes.FEATURE:
            element: "ReplicableFeature"
            old_element: "ReplicableFeature"
            if element.field_name == 'parts':
                if not old_element.is_initialized and element.is_initialized:
                    # Ready transition happened
                    edge_list = self.storage.get_incident_edges(element.element, "both")  # Feature update happened so update first hop neighborhood
                    [self.exchange(edge) for edge in edge_list if edge.source.is_initialized and edge.destination.is_initialized]

            if element.field_name == 'agg':
                res = self.apply(element.element)


class StreamingGNNInference(BaseStreamingGNNInference):
    def __init__(self, *args, **kwargs):
        super(StreamingGNNInference, self).__init__(*args, **kwargs)
        self.message_fn = None
        self.update_fn = None

    def exchange(self, edge: "SimpleEdge"):
        source: "SimpleVertex" = edge.source
        dest: "SimpleVertex" = edge.destination
        pass

    def apply(self, vertex: "SimpleVertex") -> torch.tensor:
        with torch.no_grad():
            conc = torch.concat((vertex.image.value, vertex.agg.value), dim=1)
            return self.update_fn(conc)

    def open(self, *args, **kwargs):
        self.message_fn = torch.nn.Linear(32, 16, dtype=torch.float32)
        self.update_fn = torch.nn.Sequential(
            torch.nn.Linear(32, 128),
            torch.nn.ReLU(),
            torch.nn.Linear(128, 32),
            torch.nn.ReLU(),
            torch.nn.Linear(32, 16)
        )
