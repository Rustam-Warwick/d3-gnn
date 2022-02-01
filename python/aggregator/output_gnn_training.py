import abc
from abc import ABCMeta
from aggregator import BaseAggregator
from aggregator.output_gnn_prediction import BaseOutputPrediction
from elements import GraphElement, GraphQuery, ElementTypes
from elements.vertex import BaseVertex
from elements.element_feature import ReplicableFeature
from exceptions import GraphElementNotFound
import torch
from storage.gnn_layer import GNNLayerProcess


class BaseOutputTraining(BaseAggregator, metaclass=ABCMeta):
    """ Base Class for GNN Final Layer when the predictions happen """

    def __init__(self, ident: str = "trainer", inference_name: str = "streaming_gnn", storage: "GNNLayerProcess" = None,
                 batch_size=12):
        super(BaseOutputTraining, self).__init__(ident, storage)
        self.ready = set()
        self.batch_size = batch_size
        self.inference_aggregator_name = inference_name
        self.inference_agg: "BaseOutputPrediction" = None

    @abc.abstractmethod
    def loss(self, inferences:"torch.tensor", true_values: "torch.tensor"):
        pass

    def open(self, *args, **kwargs):
        self.inference_agg = self.storage.aggregators[
            self.inference_aggregator_name]  # Have the reference to Inference aggregator


    def run(self, query: "GraphQuery", **kwargs):
        if True: return
        query.element: "ReplicableFeature"
        real_id = query.element.id
        query.element.id += '_label'
        vertex = BaseVertex(element_id=query.element.attached_to[1])
        vertex[query.element.field_name] = query.element
        vertex.attach_storage(self.storage)
        try:
            real_vertex = self.storage.get_vertex(vertex.id)
            real_vertex.external_update(vertex)
            var = real_vertex.get(real_id)  # If this guy exists
            if var is not None:
                self.ready.add(real_vertex)
                self.start_training_if_batch_filled()
        except GraphElementNotFound:
            vertex.create_element()

    def add_element_callback(self, element: "GraphElement"):
        if element.element_type is ElementTypes.FEATURE:
            return
            try:
                if element.field_name == 'feature' and self.storage.get_feature(element.id + "_label"):
                    # Already in the training set
                    #self.ready.add(self.storage.get_vertex(element.attached_to[1]))
                    #self.start_training_if_batch_filled()
                    pass
            except GraphElementNotFound:
                pass

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        pass

    def start_training_if_batch_filled(self):
        if len(self.ready) >= self.batch_size:
            # Batch size filled
            batch_embeddings = torch.vstack(list(map(lambda x: x['feature'].value, self.ready)))
            batch_labels = torch.vstack(list(map(lambda x: x['feature_label'].value, self.ready)))
            output = self.inference_agg.apply(batch_embeddings)
            self.loss(output, batch_labels)


class MyOutputTraining(BaseOutputTraining):

    def open(self, *args, **kwargs):
        super().open(*args, **kwargs)
        self.my_loss = torch.nn.CrossEntropyLoss()

    def loss(self, inferences: "torch.tensor", true_values:"torch.tensor"):
        output = self.my_loss(inferences, true_values)
        output.backward()


