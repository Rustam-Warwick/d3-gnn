import abc
from abc import ABCMeta

import flax.traverse_util
import jax.numpy
import numpy
import optax
from aggregator import BaseAggregator
from aggregator.gnn_output_inference import BaseStreamingOutputPrediction
from elements import GraphElement, GraphQuery, ElementTypes
from elements.vertex import BaseVertex
from copy import copy
from storage.gnn_layer import GNNLayerProcess


class BaseStreamingOutputTraining(BaseAggregator, metaclass=ABCMeta):
    """ Base Class for GNN Final Layer when the predictions happen """

    def __init__(self, inference_name: str = "streaming_gnn",
                 storage: "GNNLayerProcess" = None,
                 batch_size=16, epochs=5):
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
    def train(self, batch_embeddings, batch_labels):
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
            batch_embeddings = numpy.vstack(list(map(lambda x: x['feature'].value, vertices)))
            batch_labels = numpy.vstack(list(map(lambda x: x['feature_label'].value, vertices)))
            self.train(batch_embeddings, batch_labels)


class StreamingOutputTrainingJAX(BaseStreamingOutputTraining):
    def __init__(self, optimizer=optax.sgd(learning_rate=0.01, momentum=0.9), *args, **kwargs):
        super(StreamingOutputTrainingJAX, self).__init__(*args, **kwargs)
        self.optimizer = optimizer
        self.optimizer_state = None

    def open(self, *args, **kwargs):
        super(StreamingOutputTrainingJAX, self).open(*args, **kwargs)
        self.optimizer_state = self.optimizer.init(self.inference_agg.predict_fn_params.value)

    def loss(self, parameters, embedding, label):
        prediction = self.inference_agg.predict_fn.apply(parameters, embedding)
        return jax.numpy.negative(jax.numpy.dot(jax.numpy.log(prediction), label))

    def train(self, batch_embeddings, batch_labels):
        def batch_loss(parameters):
            losses = jax.vmap(self.loss, (None, 0, 0))(parameters, batch_embeddings, batch_labels)
            return jax.numpy.sum(losses)

        loss_grad_fn = jax.value_and_grad(batch_loss)
        params = self.inference_agg.predict_fn_params.value
        sum_updates = None
        for i in range(self.epochs):
            loss, grads = loss_grad_fn(params)
            updates, self.optimizer_state = self.optimizer.update(grads, self.optimizer_state)
            params = optax.apply_updates(params, updates)
            if sum_updates is None:
                sum_updates = updates
            else:
                sum_updates = jax.tree_multimap(lambda x, y: jax.numpy.asarray(x + y), sum_updates, updates)
            print("Parallell instance {%s} Epoch {%s} Loss {%s}" % (self.storage.part_id, i, loss))
        self.inference_agg.predict_fn_params.update(sum_updates)  # RPC Call
