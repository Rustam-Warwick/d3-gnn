import abc
from abc import ABCMeta
from typing import Sequence
import jax.numpy
import numpy
import optax
from aggregator import BaseAggregator
from aggregator.gnn_output_inference import BaseStreamingOutputPrediction
from elements import GraphElement, GraphQuery, IterationState, Op
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

    def update_fn_batched(self, update_params, batched_agg, batched_feature):
        return jax.vmap(self.update_fn, (None, 0, 0))(update_params, batched_agg, batched_feature)

    def backward(self, vertices, vector_grads):
        batch_aggs = numpy.vstack(list(map(lambda x: x['agg'].value[0], vertices)))
        batch_features = numpy.vstack(list(map(lambda x: x['feature'].value, vertices)))
        vertex_ids = list(map(lambda x: x.id, vertices))
        loss, grad_fn = jax.vjp(self.update_fn_batched, self.inference_agg.update_fn_params.value,
                                batch_aggs, batch_features)
        grads = grad_fn(vector_grads)
        self.inference_agg.update_fn_params.update(grads[0])  # Apply the updates for update model parameters
        backward_data = {
            "vertex_ids": vertex_ids,
            "grad_vector": grads[2]
        }
        query = GraphQuery(op=Op.AGG, element=backward_data, part=self.storage.part_id)
        query.iteration_state = IterationState.BACKWARD
        query.is_train = True
        self.storage.message(query)  # Send Backward computation
