import abc
from abc import ABCMeta
from aggregator import BaseAggregator
from elements import GraphElement, GraphQuery
from elements.element_feature import ReplicableFeature
from elements.vertex import BaseVertex
from exceptions import GraphElementNotFound
import torch
from storage.gnn_layer import GNNLayerProcess


class BaseOutputPrediction(BaseAggregator, metaclass=ABCMeta):
    """ Base Class for GNN Final Layer when the predictions happen """

    def __init__(self, ident: str = "streaming_gnn", storage: "GNNLayerProcess" = None):
        super(BaseOutputPrediction, self).__init__(ident, storage)

    @abc.abstractmethod
    def apply(self, value: "torch.tensor"):
        """ Main Function to do predictions on Predictions """
        pass

    def open(self, *args, **kwargs):
        pass

    def run(self, query: "GraphQuery", **kwargs):
        query.element: "ReplicableFeature"
        vertex = BaseVertex(element_id=query.element.attached_to[1])
        vertex[query.element.field_name] = query.element
        vertex.attach_storage(self.storage)
        try:
            real_vertex = self.storage.get_vertex(vertex.id)
            real_vertex.external_update(vertex)
        except GraphElementNotFound:
            vertex.create_element()
        print(self.apply(query.element.value))

    def add_element_callback(self, element: "GraphElement"):
        pass

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        pass


class MyOutputPrediction(BaseOutputPrediction):

    def open(self, *args, **kwargs):
        super().open(*args, **kwargs)
        self.update_fn = torch.nn.Sequential(
            torch.nn.Linear(7, 32),
            torch.nn.Linear(32, 16),
            torch.nn.Linear(16, 7),
            torch.nn.Softmax(dim=0)
        )

    def apply(self, value: "torch.tensor") -> torch.tensor:
        return self.update_fn(value)
