import abc
from abc import ABCMeta
from aggregator import BaseAggregator
from elements import GraphElement, GraphQuery
from copy import copy
from typing import TYPE_CHECKING
import torch
from storage.gnn_layer import GNNLayerProcess
if TYPE_CHECKING:
    from elements.vertex import BaseVertex


class BaseStreamingOutputPrediction(BaseAggregator, metaclass=ABCMeta):
    """ Base Class for GNN Final Layer when the predictions happen """

    def __init__(self, ident: str = "streaming_gnn", predictor: "torch.nn.Module" = None,
                 storage: "GNNLayerProcess" = None):
        super(BaseStreamingOutputPrediction, self).__init__(ident, storage)
        self.predictor = predictor

    def open(self, *args, **kwargs):
        pass

    def apply(self, vertex: "BaseVertex") -> "torch.tensor":
        feature = vertex['feature'].value
        return self.predictor(feature)

    def run(self, query: "GraphQuery", **kwargs):
        el = self.storage.get_element(query.element, False)
        if el is None:
            # Late Event
            el = copy(query.element)
            el.attach_storage(self.storage)
            el.create_element()
        el.external_update(query.element)

    def add_element_callback(self, element: "GraphElement"):
        pass

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        pass


class StreamingOutputPrediction(BaseStreamingOutputPrediction):

    def open(self, *args, **kwargs):
        super().open(*args, **kwargs)
