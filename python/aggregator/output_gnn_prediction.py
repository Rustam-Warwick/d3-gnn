import abc
from abc import ABCMeta
from aggregator import BaseAggregator
from elements import GraphElement, GraphQuery
from elements.element_feature import ReplicableFeature
from elements.vertex import BaseVertex
from exceptions import GraphElementNotFound
import torch
from storage.gnn_layer import GNNLayerProcess


class BaseStreamingOutputPrediction(BaseAggregator, metaclass=ABCMeta):
    """ Base Class for GNN Final Layer when the predictions happen """

    def __init__(self, ident: str = "streaming_gnn", predict_fn: "torch.nn.Module" = None,
                 storage: "GNNLayerProcess" = None):
        super(BaseStreamingOutputPrediction, self).__init__(ident, storage)
        self.predict_fn = predict_fn

    def open(self, *args, **kwargs):
        pass

    def run(self, query: "GraphQuery", **kwargs):
        query.element: "BaseVertex"
        query.element.attach_storage(self.storage)
        try:
            real_vertex = self.storage.get_vertex(query.element.id)
            real_vertex.external_update(query.element)
        except GraphElementNotFound:
            query.element.create_element()

    def add_element_callback(self, element: "GraphElement"):
        pass

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        pass


class StreamingOutputPrediction(BaseStreamingOutputPrediction):

    def open(self, *args, **kwargs):
        super().open(*args, **kwargs)
