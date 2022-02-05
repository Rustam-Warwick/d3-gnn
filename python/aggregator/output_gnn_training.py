import abc
from abc import ABCMeta

import jax.numpy

from aggregator import BaseAggregator
from aggregator.output_gnn_prediction import BaseStreamingOutputPrediction
from elements import GraphElement, GraphQuery, ElementTypes
from elements.vertex import BaseVertex
from copy import copy
from jax import vmap
from storage.gnn_layer import GNNLayerProcess


class BaseStreamingOutputTraining(BaseAggregator, metaclass=ABCMeta):
    """ Base Class for GNN Final Layer when the predictions happen """

    def __init__(self, inference_name: str = "streaming_gnn",
                 storage: "GNNLayerProcess" = None,
                 batch_size=3):
        super(BaseStreamingOutputTraining, self).__init__("trainer", storage)
        self.ready = set()  # Ids of vertices that have both features and labels
        self.batch_size = batch_size  # Size of self.ready when training should be triggered
        self.inference_aggregator_name = inference_name  # Name of the inferences to which this is attached
        self.inference_agg: "BaseStreamingOutputPrediction" = None   # Reference to inference created on open()

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
            batch_embeddings = list(map(lambda x: x['feature'].value, vertices))
            batch_labels = list(map(lambda x: x['feature_label'].value, vertices))
            self.train(batch_embeddings, batch_labels)


class StreamingOutputTraining(BaseStreamingOutputTraining):
    def __init__(self, *args, **kwargs):
        super(StreamingOutputTraining, self).__init__(*args, **kwargs)

    def loss(self, prediction, label):
        return jax.numpy.negative(jax.numpy.dot(jax.numpy.log(prediction), label))

    def train(self, batch_embeddings, batch_labels):
        prediction = self.inference_agg.predict(batch_embeddings[0])
        mloss = self.loss(prediction, batch_labels[0])
        print(mloss)
        pass

