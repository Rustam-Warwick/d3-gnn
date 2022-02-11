import abc
from abc import ABCMeta
from typing import Sequence
import jax.numpy
import numpy
import optax
from aggregator import BaseAggregator
from aggregator.gnn_output_inference import BaseStreamingOutputPrediction
from elements.element_feature.tensor_feature import TensorReplicableFeature
from elements import GraphElement, GraphQuery, IterationState, Op, ElementTypes, ReplicaState
from storage.gnn_layer import GNNLayerProcess


class BaseStreamingLayerTraining(BaseAggregator, metaclass=ABCMeta):
    """ Base Class for GNN Final Layer when the predictions happen """

    def __init__(self, inference_name: str = "streaming_gnn",
                 storage: "GNNLayerProcess" = None, epochs=5):
        super(BaseStreamingLayerTraining, self).__init__("trainer", storage)
        self.inference_aggregator_name = inference_name  # Name of the inferences to which this training is attached
        self.inference_agg: "BaseStreamingOutputPrediction" = None  # Reference to inference. Created on open()
        self.epochs = epochs  # Number of epochs on batch of training should go

    def open(self, *args, **kwargs):
        self.inference_agg = self.storage.aggregators[
            self.inference_aggregator_name]  # Have the reference to Inference aggregator

    @abc.abstractmethod
    def backward(self, vertices, vector_grads):
        """ Given array of vertices and gradient of vertices embedding w.r.t. loss function do the backprop and send
        grad backward """
        pass

    def run(self, query: "GraphQuery", **kwargs):
        vertex_ids = query.element['vertex_ids']
        vertices = list(map(lambda x: self.storage.get_vertex(x), vertex_ids))
        vector_grads = query.element["grad_vector"]
        self.backward(vertices, vector_grads)

    def add_element_callback(self, element: "GraphElement"):
        pass

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        pass


class StreamingLayerTrainingJAX(BaseStreamingLayerTraining):
    def __init__(self, *args, **kwargs):
        super(StreamingLayerTrainingJAX, self).__init__(*args, **kwargs)

    def open(self, *args, **kwargs):
        super(StreamingLayerTrainingJAX, self).open(*args, **kwargs)

    def update_fn(self, update_params, agg, feature):
        conc = jax.numpy.concatenate((feature, agg))
        return self.inference_agg.update_fn.apply(update_params, conc)

    def message_fn(self, message_params, feature):
        return self.inference_agg.message_fn.apply(message_params, feature)

    def add_element_callback(self, element: "GraphElement"):
        if element.element_type is ElementTypes.FEATURE and element.field_name == 'agg_grads':
            element: "TensorReplicableFeature"
            vertex = self.storage.get_vertex(element.attached_to[1])
            agg = vertex['agg']
            in_edges = self.storage.get_incident_edges(vertex, "in")
            source_list = list(filter(lambda x: x.is_initialized, [e.source for e in in_edges]))
            if len(source_list) > 0:
                in_features = jax.numpy.vstack([e["feature"].value for e in source_list])
                loss, grad_fn = jax.vjp(lambda params, feature: agg.fn(jax.vmap(self.message_fn, [None, 0])(params, feature)), self.inference_agg.message_fn_params.value, in_features)
                message_fn_params, feature_grad = grad_fn(element.value)
                self.inference_agg.message_fn_params.update(message_fn_params) # Update message_fn_params

                back_messages = dict()
                for i, vertex in enumerate(source_list):
                    if vertex.master_part in back_messages:
                        instance = back_messages[vertex.master_part]
                        instance['vertex_ids'].append(vertex.id)
                        instance['grad_vector'] = jax.numpy.vstack((instance['grad_vector'], feature_grad[i]))
                    else:
                        back_messages[vertex.master_part] = {
                            "vertex_ids": [vertex.id],
                            "grad_vector": feature_grad[i, None]
                        }

                for part, backward_data in back_messages.items():
                    query = GraphQuery(op=Op.AGG, element=backward_data, part=part)
                    query.iteration_state = IterationState.BACKWARD
                    query.is_train = True
                    self.storage.message(query)  # Send Backward computation

    def backward(self, vertices, vector_grads):
        batch_aggregations = numpy.vstack(list(map(lambda x: x['agg'].value[0], vertices)))
        batch_features = numpy.vstack(list(map(lambda x: x['feature'].value, vertices)))
        vertex_ids = list(map(lambda x: x.id, vertices))
        loss, grad_fn = jax.vjp(jax.vmap(self.update_fn, (None, 0, 0)), self.inference_agg.update_fn_params.value,
                                batch_aggregations, batch_features)
        update_fn_grads, agg_grad, feature_grad = grad_fn(vector_grads)
        for i, vertex in enumerate(vertices):
            vertex['agg_grads'] = TensorReplicableFeature(value=agg_grad[i])
            vertex['agg_grads'].sync_replicas()
        self.inference_agg.update_fn_params.update(update_fn_grads)  # Apply the updates for update model parameters
        backward_data = {
            "vertex_ids": vertex_ids,
            "grad_vector": feature_grad
        }
        query = GraphQuery(op=Op.AGG, element=backward_data, part=self.storage.part_id)
        query.iteration_state = IterationState.BACKWARD
        query.is_train = True
        self.storage.message(query)  # Send Backward computation
