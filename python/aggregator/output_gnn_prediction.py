import abc
from abc import ABCMeta
from aggregator import BaseAggregator
from elements import GraphElement, GraphQuery
from copy import copy
from typing import TYPE_CHECKING
from flax.linen import Module, softmax
from storage.gnn_layer import GNNLayerProcess
if TYPE_CHECKING:
    from elements.vertex import BaseVertex


class BaseStreamingOutputPrediction(BaseAggregator, metaclass=ABCMeta):
    """ Base Class for GNN Final Layer when the predictions happen """

    def __init__(self, ident: str = "streaming_gnn", storage: "GNNLayerProcess" = None):
        super(BaseStreamingOutputPrediction, self).__init__(ident, storage)

    def open(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def predict(self, feature):
        pass

    def _predict(self, vertex: "BaseVertex"):
        feature = vertex['feature'].value
        return self.predict(feature)

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


class StreamingOutputPredictionJAX(BaseStreamingOutputPrediction):
    def __init__(self, predict_fn: "Module", predict_fn_params, *args, **kwargs):
        super(StreamingOutputPredictionJAX, self).__init__(*args, **kwargs)
        self.predict_fn = predict_fn
        self.predict_fn_params = predict_fn_params

    def predict(self, feature):
        return softmax(self.predict_fn.apply(self.predict_fn_params, feature))

    def open(self, *args, **kwargs):
        super().open(*args, **kwargs)
