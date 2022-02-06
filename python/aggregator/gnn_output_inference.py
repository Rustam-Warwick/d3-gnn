import abc
from abc import ABCMeta
from aggregator import BaseAggregator
from elements import GraphElement, GraphQuery, ElementTypes
from elements.element_feature.jax_params import JaxParamsFeature
from copy import copy
from typing import TYPE_CHECKING
from flax.linen import Module
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
        # attached to one element
        params_feature = JaxParamsFeature(predict_fn_params, master=0, element_id=self.id)
        self.predict_fn_params = params_feature

    def predict(self, feature):
        return self.predict_fn.apply(self.predict_fn_params.value, feature)

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        super(StreamingOutputPredictionJAX, self).update_element_callback(element, old_element)
        if element.element_type is ElementTypes.FEATURE and element.field_name == self.id:
            self.predict_fn_params = element  # Update(cache) the old value

    def open(self, *args, **kwargs):
        super().open(*args, **kwargs)
        self.predict_fn_params.attach_storage(self.storage)
        self.predict_fn_params.create_element()
