import abc
from abc import ABCMeta
from aggregator import BaseAggregator
from elements import GraphQuery, RPCDestination, IterationState
from elements.element_feature.jax_params import JaxParamsFeature
from elements.element_feature.tensor_feature import TensorReplicableFeature
from copy import copy
from exceptions import GraphElementNotFound
from typing import TYPE_CHECKING
from flax.linen import Module

if TYPE_CHECKING:
    from elements.vertex import BaseVertex


class BaseStreamingOutputPrediction(BaseAggregator, metaclass=ABCMeta):
    """ Base Class for GNN Final Layer when the predictions happen """

    @abc.abstractmethod
    def predict(self, vertex):
        pass

    def run(self, *args, **kwargs):
        pass

    def forward(self, vertex_id, feature):
        try:
            vertex = self.storage.get_vertex(vertex_id)
            vertex['feature'].update_value(feature)  # Update value
        except GraphElementNotFound:
            vertex = BaseVertex(master=self.part_id)
            vertex.id = vertex_id
            vertex.attach_storage(self.storage)
            vertex['feature'] = TensorReplicableFeature(value=feature)
            vertex.create_element()


class StreamingOutputPredictionJAX(BaseStreamingOutputPrediction):
    def __init__(self, predict_fn: "Module", predict_fn_params, *args, **kwargs):
        super(StreamingOutputPredictionJAX, self).__init__(*args, **kwargs)
        self.predict_fn = predict_fn
        # attached to one element
        self['predict_params'] = JaxParamsFeature(value=predict_fn_params)

    def predict(self, vertex):
        feature = vertex['feature'].value
        return self.predict_fn.apply(self['predict_params'].value, feature)
