import abc
from abc import ABCMeta

from typing import Sequence
import jax.numpy
import numpy
from decorators import rpc
from aggregator import BaseAggregator
from aggregator.gnn_output_inference import BaseStreamingOutputPrediction
from elements import GraphElement, GraphQuery, ElementTypes, IterationState, RPCDestination
from elements.vertex import BaseVertex
from copy import copy


class BaseStreamingOutputTraining(BaseAggregator, metaclass=ABCMeta):
    """ Base Class for GNN Final Layer when the predictions happen """

    def __init__(self, inference_agg: "BaseStreamingOutputPrediction", batch_size=3, epochs=5, *args, **kwargs):
        super(BaseStreamingOutputTraining, self).__init__(element_id="trainer", *args, **kwargs)
        self.ready = set()  # Ids of vertices that have both features and labels, hence ready to be trained on
        self.batch_size = batch_size  # Size of self.ready when training should be triggered
        self.inference_agg: "BaseStreamingOutputPrediction" = inference_agg  # Reference to inference. Created on open()
        self.epochs = epochs  # Number of epochs on batch of training should go

    @abc.abstractmethod
    def train(self, vertices: Sequence["BaseVertex"]):
        pass

    @rpc(is_procedure=True, iteration=IterationState.BACKWARD, destination=RPCDestination.SELF)
    def backward(self, vertex_ids: Sequence[str], grad_vector: jax.numpy.array):
        """ Since this is the starting point and it is being sent backwards no implementation needed for this """
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

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        super(BaseStreamingOutputTraining, self).update_element_callback(element, old_element)
        if element.element_type is ElementTypes.VERTEX and element.id not in self.ready:
            if element.get('feature') and element.get('feature_label'):  # If both labels and predictions exist
                self.ready.add(element.id)
                self.start_training_if_batch_filled()

    def start_training_if_batch_filled(self):
        if len(self.ready) >= self.batch_size:
            # Batch size filled
            vertices = list(map(lambda x: self.storage.get_vertex(x), self.ready))
            self.ready.clear()
            self.train(vertices)


class StreamingOutputTrainingJAX(BaseStreamingOutputTraining):
    def __init__(self, *args, **kwargs):
        super(StreamingOutputTrainingJAX, self).__init__(*args, **kwargs)
        self.learning_rate = 0.04

    def open(self, *args, **kwargs):
        super(StreamingOutputTrainingJAX, self).open(*args, **kwargs)

    def loss(self, parameters, embedding, label):
        prediction = self.inference_agg.predict_fn.apply(parameters, embedding)
        return -jax.numpy.sum(label * jax.numpy.log(prediction))

    def batch_loss(self, parameters, batch_embeddings, batch_labels):
        losses = jax.vmap(self.loss, (None, 0, 0))(parameters, batch_embeddings, batch_labels)
        return jax.numpy.sum(losses)

    def train(self, vertices: Sequence["BaseVertex"]):
        # @todo Sometimes the gradients are becoming zero, why it is happening, double-check
        batch_embeddings = numpy.vstack(list(map(lambda x: x['feature'].value, vertices)))
        batch_labels = numpy.vstack(list(map(lambda x: x['feature_label'].value, vertices)))
        vertex_ids = list(map(lambda x: x.id, vertices))

        def lambda_wrapper(p, e):
            return self.batch_loss(p, e, batch_labels)

        loss, grad_fn = jax.vjp(lambda_wrapper, self.inference_agg['predict_params'].value,
                                batch_embeddings)  # Grad of [
        # parameters, inputs]
        print("Loss is {%s}" % (loss))
        predict_grads, embedding_grads = jax.tree_map(lambda x: x * self.learning_rate, grad_fn(1.))
        self.inference_agg['predict_params'].update(predict_grads)  # Apply the updates for model parameters
        self.backward(vertex_ids, embedding_grads)
