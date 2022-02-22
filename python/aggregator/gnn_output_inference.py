import abc
import copy
from abc import ABCMeta
from aggregator import BaseAggregator
from elements import GraphQuery, RPCDestination, IterationState
from elements.vertex import BaseVertex
from elements.element_feature.jax_params import JaxParamsFeature
from elements.element_feature.tensor_feature import TensorReplicableFeature
from decorators import rpc
from exceptions import GraphElementNotFound
from flax.linen import Module


class BaseStreamingOutputPrediction(BaseAggregator, metaclass=ABCMeta):
    """ Base Class for GNN Final Layer when the predictions happen """

    @abc.abstractmethod
    def predict(self, feature, *args, **kwargs):
        pass

    def run(self, *args, **kwargs):
        pass

    @rpc(is_procedure=True, destination=RPCDestination.SELF, iteration=IterationState.ITERATE)
    def forward(self, vertex_id, feature, part_id):
        try:
            vertex = self.storage.get_vertex(vertex_id)
            copy_vertex = copy.copy(vertex)
            copy_vertex['feature'] = TensorReplicableFeature(value=feature)
            vertex.update_element(copy_vertex)
        except GraphElementNotFound:
            vertex = BaseVertex(master=self.part_id)
            vertex.id = vertex_id
            vertex['feature'] = TensorReplicableFeature(value=feature)
            vertex.attach_storage(self.storage)
            vertex.create_element()


class StreamingOutputPredictionJAX(BaseStreamingOutputPrediction):
    def __init__(self, predict_fn: "Module", predict_fn_params, *args, **kwargs):
        super(StreamingOutputPredictionJAX, self).__init__(*args, **kwargs)
        self.predict_fn = predict_fn
        # attached to one element
        self['predict_params'] = JaxParamsFeature(value=predict_fn_params)

    def predict(self, feature, params):
        return self.predict_fn.apply(params, feature)
