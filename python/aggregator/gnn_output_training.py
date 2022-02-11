import abc
from abc import ABCMeta

from typing import Sequence
import jax.numpy
import numpy
import optax
from aggregator import BaseAggregator
from aggregator.gnn_output_inference import BaseStreamingOutputPrediction
from elements import GraphElement, GraphQuery, ElementTypes, IterationState, Op
from elements.vertex import BaseVertex
from copy import copy
from storage.gnn_layer import GNNLayerProcess


class BaseStreamingOutputTraining(BaseAggregator, metaclass=ABCMeta):
    """ Base Class for GNN Final Layer when the predictions happen """

    def __init__(self, inference_name: str = "streaming_gnn",
                 storage: "GNNLayerProcess" = None,
                 batch_size=3, epochs=5):
        super(BaseStreamingOutputTraining, self).__init__("trainer", storage)
        self.ready = set()  # Ids of vertices that have both features and labels, hence ready to be trained on
        self.batch_size = batch_size  # Size of self.ready when training should be triggered
        self.inference_aggregator_name = inference_name  # Name of the inferences to which this training is attached
        self.inference_agg: "BaseStreamingOutputPrediction" = None  # Reference to inference. Created on open()
        self.epochs = epochs  # Number of epochs on batch of training should go

    def open(self, *args, **kwargs):
        self.inference_agg = self.storage.aggregators[
            self.inference_aggregator_name]  # Have the reference to Inference aggregator

    @abc.abstractmethod
    def train(self, vertices: Sequence["BaseVertex"]):
        pass

    def run(self, query: "GraphQuery", **kwargs):
        vertex: "BaseVertex" = query.element
        ft = vertex['feature']
        vertex._features.clear()
        vertex['feature_label'] = ft
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
        if element.element_type is ElementTypes.VERTEX and element.id not in self.ready:
            if element.get('feature') and element.get('feature_label'):  # If both labels and predictions exist
                self.ready.add(element.id)
                self.start_training_if_batch_filled()

    def start_training_if_batch_filled(self):
        if len(self.ready) >= self.batch_size:
            # Batch size filled
            vertices = list(map(lambda x: self.storage.get_vertex(x), self.ready))
            self.train(vertices)
            self.ready.clear()


class StreamingOutputTrainingJAX(BaseStreamingOutputTraining):
    def __init__(self, optimizer=optax.sgd(learning_rate=0.01, momentum=0.9), *args, **kwargs):
        super(StreamingOutputTrainingJAX, self).__init__(*args, **kwargs)
        self.optimizer = optimizer
        self.optimizer_state = None
        self.learning_rate = 0.04

    def open(self, *args, **kwargs):
        super(StreamingOutputTrainingJAX, self).open(*args, **kwargs)
        self.optimizer_state = self.optimizer.init(self.inference_agg.predict_fn_params.value)

    def loss(self, parameters, embedding, label):
        prediction = self.inference_agg.predict_fn.apply(parameters, embedding)
        return -jax.numpy.sum(label * jax.numpy.log(prediction))

    def batch_loss(self, parameters, batch_embeddings, batch_labels):
        losses = jax.vmap(self.loss, (None, 0, 0))(parameters, batch_embeddings, batch_labels)
        return jax.numpy.sum(losses)

    def train(self, vertices: Sequence["BaseVertex"]):
        batch_embeddings = numpy.vstack(list(map(lambda x: x['feature'].value, vertices)))
        batch_labels = numpy.vstack(list(map(lambda x: x['feature_label'].value, vertices)))
        vertex_ids = list(map(lambda x: x.id, vertices))

        def lambda_wrapper(p, e):
            return self.batch_loss(p, e, batch_labels)

        loss, grad_fn = jax.vjp(lambda_wrapper, self.inference_agg.predict_fn_params.value, batch_embeddings)  # Grad of [
        # parameters, inputs]
        grads = jax.tree_map(lambda x: x * self.learning_rate, grad_fn(1.))
        self.inference_agg.predict_fn_params.update(grads[0])  # Apply the updates for model parameters
        backward_data = {
            "vertex_ids": vertex_ids,
            "grad_vector": grads[1]
        }
        print("Loss is {%s}" % (loss, ))
        query = GraphQuery(op=Op.AGG, element=backward_data, part=self.storage.part_id)
        query.iteration_state = IterationState.BACKWARD
        query.is_train = True
        self.storage.message(query)  # Send Backward computation
