import abc
from abc import ABCMeta

from typing import Sequence
import jax.numpy
import numpy
import optax

from decorators import rpc
from aggregator import BaseAggregator
from aggregator.gnn_output_inference import BaseStreamingOutputPrediction
from elements import GraphElement, GraphQuery, ElementTypes, IterationState, RPCDestination
from elements.vertex import BaseVertex
from copy import copy


class BaseStreamingOutputTraining(BaseAggregator, metaclass=ABCMeta):
    """ Base Class for GNN Final Layer when the predictions happen """

    def __init__(self, inference_agg: "BaseStreamingOutputPrediction", batch_size=2, *args, **kwargs):
        super(BaseStreamingOutputTraining, self).__init__(element_id="trainer", *args, **kwargs)
        self.ready = set()  # Ids of vertices that have both features and labels, hence ready to be trained on
        self.batch_size = batch_size  # Size of self.ready when training should be triggered
        self.inference_agg: "BaseStreamingOutputPrediction" = inference_agg  # Reference to inference. Created on open()
        self.msg_received = set()
        self.predict_grad_list = list()

    @abc.abstractmethod
    def train(self, vertices: Sequence["BaseVertex"]):
        pass

    @rpc(is_procedure=True, iteration=IterationState.BACKWARD, destination=RPCDestination.SELF)
    def backward(self, vertex_ids: Sequence[str], grad_vector: jax.numpy.array, part_id, part_version):
        """ Since this is the starting point and it is being sent backwards no implementation needed for this """
        pass

    @rpc(is_procedure=True)
    def update_model(self, update_grad_acc, part_id, part_version):
        self.msg_received.add(part_id)
        self.predict_grad_list.append(update_grad_acc)
        if len(self.msg_received) == self.storage.parallelism:
            self.predict_grad_list = list(filter(lambda x: x is not None, self.predict_grad_list))
            self.inference_agg['predict_params'].batch_update(*self.predict_grad_list)
            self.msg_received.clear()
            self.predict_grad_list.clear()

    def run(self, query: "GraphQuery", **kwargs):
        vertex: "BaseVertex" = query.element
        ft = vertex['feature']
        vertex._features.clear()
        vertex['feature_label'] = ft
        el = self.storage.get_element(vertex, False)
        if el is None:
            # Late Event
            el = vertex
            el.attach_storage(self.storage)
            el.create_element()
        else:
            el.external_update(query.element)

    def add_element_callback(self, element: "GraphElement"):
        super(BaseStreamingOutputTraining, self).add_element_callback(element)
        if element.element_type is ElementTypes.FEATURE:
            if element.element.id not in self.ready and element.element.get('feature_label') and element.element.get(
                    'feature'):
                self.ready.add(element.element.id)
                self.start_training_if_batch_filled()

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        super(BaseStreamingOutputTraining, self).update_element_callback(element, old_element)
        if element.element_type is ElementTypes.VERTEX:
            if element.id not in self.ready and element.get('feature') and element.get('feature_label'):
                self.ready.add(element.id)
                self.start_training_if_batch_filled()
        if element.element_type is ElementTypes.FEATURE and element.field_name == 'feature':
            # Feature update
            if element.element.id not in self.ready and element.element.get('feature_label') and element.element.get(
                    'feature'):
                self.ready.add(element.element.id)
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
        self.learning_rate = 0.01
        params_dict = self.inference_agg['predict_params'].value
        self.grad_acc = jax.tree_map(lambda x: jax.numpy.zeros_like(x), params_dict)

    def on_watermark(self):
        self.update_model(self.grad_acc)
        self.grad_acc = jax.tree_map(lambda x: jax.numpy.zeros_like(x), self.grad_acc)

    def loss(self, parameters, embedding, label):
        prediction = self.inference_agg.predict(embedding, parameters)
        return optax.softmax_cross_entropy(prediction, label)

    def batch_loss(self, parameters, batch_embeddings, batch_labels):
        losses = jax.vmap(self.loss, (None, 0, 0))(parameters, batch_embeddings, batch_labels)
        return jax.numpy.mean(losses)

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
        print("Loss is {%s}" % loss)
        # print("Loss is {%s}" % loss)
        predict_grads, embedding_grads = jax.tree_map(lambda x: x * self.learning_rate, grad_fn(1.))
        self.grad_acc = jax.tree_multimap(lambda *x: jax.numpy.sum(jax.numpy.vstack(x), axis=0), self.grad_acc,
                                          predict_grads)
        # self.inference_agg['predict_params'].update(predict_grads)  # Apply the updates for model parameters
        self.backward(vertex_ids, embedding_grads)
