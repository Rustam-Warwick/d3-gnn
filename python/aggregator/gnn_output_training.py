import abc
from abc import ABCMeta

from typing import Sequence
import jax.numpy
import numpy
import optax

from decorators import rmi
from aggregator import BaseAggregator
from aggregator.gnn_output_inference import BaseStreamingOutputPrediction
from elements import GraphElement, GraphQuery, ElementTypes, IterationState, RPCDestination
from elements.vertex import BaseVertex
from copy import copy


class BaseStreamingOutputTraining(BaseAggregator, metaclass=ABCMeta):
    """ Base Class for GNN Final Layer when the predictions happen """

    def __init__(self, inference_agg: "BaseStreamingOutputPrediction", batch_size=16, *args, **kwargs):
        super(BaseStreamingOutputTraining, self).__init__(element_id="trainer", *args, **kwargs)
        self.ready = set()  # Ids of vertices that have both features and labels, hence ready to be trained on
        self.batch_size = batch_size  # Size of self.ready when training should be triggered
        self.inference_agg: "BaseStreamingOutputPrediction" = inference_agg  # Reference to inference. Created on open()

    # def open(self, runtime_context):
    #     self.meter = runtime_context.get_metric_group().meter("loss", time_span_in_seconds=120)

    @abc.abstractmethod
    def update_model(self, grad_acc, *args, **kwargs):
        """ Update the model given accumulate grads from part """
        pass

    @abc.abstractmethod
    def train(self, vertices: Sequence["BaseVertex"]):
        pass

    @rmi(is_procedure=True, iteration=IterationState.BACKWARD, destination=RPCDestination.SELF)
    def backward(self, vertex_ids: Sequence[str], grad_vector: jax.numpy.array, part_id, part_version):
        """ Since this is the starting point, and it is being sent backwards no implementation needed for this """
        pass

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
                    'feature') and element.element.get("feature").is_ready:
                self.ready.add(element.element.id)
                self.start_training_if_batch_filled()

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        if element.element_type is ElementTypes.FEATURE:
            if element.element.id not in self.ready and element.element.get('feature_label') and element.element.get(
                    'feature') and element.element.get("feature").is_ready:
                self.ready.add(element.element.id)
                self.start_training_if_batch_filled()
            if element.field_name == 'params':
                self.storage.part_version += 1
                self.ready.clear()

    def start_training_if_batch_filled(self):
        if len(self.ready) >= self.batch_size:
            # Batch size filled
            vertices = list(map(lambda x: self.storage.get_vertex(x), self.ready))
            self.ready.clear()
            self.train(vertices)


class StreamingOutputTrainingJAX(BaseStreamingOutputTraining):
    def __init__(self, *args, **kwargs):
        super(StreamingOutputTrainingJAX, self).__init__(*args, **kwargs)
        self.learning_rate = 0.1
        self.grad_acc = None  # Local Gradient Accumulator
        self.msg_received = set()  # Master part messages received
        self.grad_list = list()  # List of the grad messages received

    @rmi(is_procedure=True)
    def update_model(self, update_grad_acc, part_id, part_version):
        self.msg_received.add(part_id)
        self.grad_list.append(update_grad_acc)
        if len(self.msg_received) == self.storage.parallelism:
            self.grad_list = list(filter(lambda x: x is not None, self.grad_list))
            self.grad_list.append(
                jax.tree_multimap(lambda x: jax.numpy.zeros_like(x), self.inference_agg['params'].value))
            sum_grads = jax.tree_multimap(lambda *x: jax.numpy.sum(jax.numpy.vstack(x), axis=0), *self.grad_list)
            self.inference_agg['params'].update(sum_grads)
            self.msg_received.clear()
            self.grad_list.clear()
            print("Model Updated")

    def on_watermark(self):
        self.update_model(self.grad_acc)
        self.grad_acc = None

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

        loss, grad_fn = jax.vjp(lambda p, e: self.batch_loss(p, e, batch_labels),
                                self.inference_agg['params'].value,
                                batch_embeddings)

        print("Loss is {%s}" % loss)
        predict_grads, embedding_grads = jax.tree_map(lambda x: x * self.learning_rate, grad_fn(1.))
        if self.grad_acc is None:
            self.grad_acc = predict_grads
        else:
            self.grad_acc = jax.tree_multimap(lambda *x: jax.numpy.sum(jax.numpy.vstack(x), axis=0), self.grad_acc,
                                              predict_grads)

        # self.inference_agg['predict_params'].update(predict_grads)  # Apply the updates for model parameters
        if embedding_grads.any():
            # If embedding is all zeros no need to do backprop
            self.backward(vertex_ids, embedding_grads)
