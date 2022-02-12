import abc
from abc import ABCMeta
from aggregator import BaseAggregator
from elements import GraphElement, GraphQuery, ElementTypes
from elements.element_feature.jax_params import JaxParamsFeature
from copy import copy
import jax
from typing import TYPE_CHECKING
from flax.linen import Module
from storage.gnn_layer import GNNLayerProcess

if TYPE_CHECKING:
    from elements.vertex import BaseVertex


class BaseStreamingOutputPrediction(BaseAggregator, metaclass=ABCMeta):
    """ Base Class for GNN Final Layer when the predictions happen """

    @abc.abstractmethod
    def predict(self, feature):
        pass

    def run(self, query: "GraphQuery", **kwargs):
        el = self.storage.get_element(query.element, False)
        if el is None:
            # Late Event
            el = copy(query.element)
            el.attach_storage(self.storage)
            el.create_element()
        el.external_update(query.element)

class StreamingOutputPredictionJAX(BaseStreamingOutputPrediction):
    def __init__(self, predict_fn: "Module", predict_fn_params, *args, **kwargs):
        super(StreamingOutputPredictionJAX, self).__init__(*args, **kwargs)
        self.predict_fn = predict_fn
        # attached to one element
        self['predict_params'] = JaxParamsFeature(value=predict_fn_params)

    def predict(self, vertex):
        feature = vertex['feature'].value
        return self.predict_fn.apply(self['predict_params'].value, feature)
