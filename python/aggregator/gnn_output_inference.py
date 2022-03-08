import abc
import jax
from abc import ABCMeta
from aggregator import BaseAggregator
from elements import GraphQuery, RPCDestination, IterationState, ElementTypes
from elements.vertex import BaseVertex
from elements.element_feature import ReplicableFeature
from elements.element_feature.jax_params import JaxParamsFeature
from elements.element_feature.tensor_feature import VersionedTensorReplicableFeature
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

    @rpc(is_procedure=True, destination=RPCDestination.SELF, iteration=IterationState.FORWARD)
    def forward(self, vertex_id, feature, part_id, part_version):
        if part_version < self.storage.part_version:
            # Previous is behind ignore this message
            # New embedding should come in soon
            pass
        else:
            try:
                vertex = self.storage.get_vertex(vertex_id)
                if vertex.get("feature"):
                    # Feature exists
                    vertex['feature'].version = part_version
                    vertex['feature'].update_value(feature)  # Update value
                else:
                    vertex['feature'] = VersionedTensorReplicableFeature(value=feature, version=part_version)
            except GraphElementNotFound:
                vertex = BaseVertex(master=self.part_id)
                vertex.id = vertex_id
                vertex['feature'] = VersionedTensorReplicableFeature(value=feature, version=part_version)
                vertex.attach_storage(self.storage)
                vertex.create_element()

    def add_element_callback(self, element: "GraphElement"):
        if element.element_type is ElementTypes.FEATURE:
            feature:"ReplicableFeature" = element
            if feature.field_name == 'feature':
                predict = self.predict(feature.value, self['params'].value)
                print(predict)

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        if element.element_type is ElementTypes.FEATURE:
            feature:"ReplicableFeature" = element
            if feature.field_name == 'feature':
                predict = self.predict(feature.value, self['params'].value)
                print(predict)

class StreamingOutputPredictionJAX(BaseStreamingOutputPrediction):
    def __init__(self, predict_fn: "Module", predict_fn_params, *args, **kwargs):
        super(StreamingOutputPredictionJAX, self).__init__(*args, **kwargs)
        self.predict_fn = predict_fn
        # attached to one element
        self['params'] = JaxParamsFeature(value=predict_fn_params)

    def predict(self, feature, params):
        return self.predict_fn.apply(params, feature)
