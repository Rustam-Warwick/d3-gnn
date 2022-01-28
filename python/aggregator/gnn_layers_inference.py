import abc
from aggregator import BaseAggregator
from elements import ElementTypes, GraphQuery, Op, ReplicaState
from elements.element_feature.tensor_feature import MeanAggregatorReplicableFeature, AggregatorFeatureMixin, \
    TensorReplicableFeature
import torch
from abc import ABCMeta

from elements.edge import BaseEdge
from elements.element_feature import ReplicableFeature
from elements.vertex import BaseVertex
from elements import GraphElement
from storage.gnn_layer import GNNLayerProcess


class BaseStreamingGNNInference(BaseAggregator, metaclass=ABCMeta):
    """ Base class for stream gnn inference. Represents the embedding(L) Layers of GNN Aggregation @param
    layers_covered represents the depth of how many layers is this GNN Inference covering: Ex: If layers_covered = 2,
    it means that Layer=0, & and Layer=1 are going to happen in this horizontal operator and then pushed to next ones
    """

    def __init__(self, layers_covered=1, ident: str = "streaming_gnn", storage: "GNNLayerProcess" = None):
        super(BaseStreamingGNNInference, self).__init__(ident, storage)
        self.layers_covered = layers_covered

    @abc.abstractmethod
    def exchange(self, edge: "BaseEdge") -> "torch.tensor":
        """ Enumerate each edge Increment the aggregate function """
        pass

    @abc.abstractmethod
    def apply(self, vertex: "BaseVertex") -> "torch.tensor":
        pass

    def run(self, query: "GraphQuery", *args, **kwargs):
        """ Take in the incoming aggregation result and add it to the storage engine, res is in callbacks """
        new_feature: "ReplicableFeature" = query.element
        old_feature = self.storage.get_feature(new_feature.id)
        if old_feature is None:
            self.storage.add_element(new_feature)
        else:
            old_feature.external_update(new_feature)

    def add_element_callback(self, element: "GraphElement"):
        if element.element_type is ElementTypes.VERTEX and element.state is ReplicaState.MASTER:
            # Populate the required features for embeddings
            element['agg'] = MeanAggregatorReplicableFeature(
                tensor=torch.zeros((32,), dtype=torch.float32, requires_grad=False), is_halo=True)
            element['feature'] = TensorReplicableFeature(value=torch.zeros((7,), requires_grad=False, dtype=torch.float32))
        if element.element_type is ElementTypes.EDGE:
            element: "BaseEdge"
            if element.source.is_initialized and element.destination.is_initialized:
                # If vertices are ready do the embedding now
                try:
                    with torch.no_grad():
                        msg = self.exchange(element)
                    agg: "AggregatorFeatureMixin" = element.destination['agg']
                    agg.reduce(msg)  # Update the aggregator
                except KeyError:
                    pass  # One of features is not here yet

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        if element.element_type is ElementTypes.VERTEX:
            if not old_element.is_initialized and element.is_initialized:
                # Bulk Reduce for all waiting nodes
                with torch.no_grad():
                    self.reduce_all_edges(element)
        if element.element_type is ElementTypes.FEATURE:
            element: "ReplicableFeature"
            old_element: "ReplicableFeature"
            if element.field_name == 'feature':
                # Do something new
                old_vertex = BaseVertex(element_id=element.element.id)
                old_vertex["feature"] = old_element
                with torch.no_grad():
                    self.update_all_edges(element.element, old_vertex)

            if element.field_name == 'agg' and element.state is ReplicaState.MASTER:
                # Generate new embedding for the node & send to master part of the next layer
                with torch.no_grad():
                    embedding = self.apply(element.element)
                vertex = BaseVertex(element_id=element.element.id)
                vertex["feature"] = TensorReplicableFeature(value=embedding)
                query = GraphQuery(Op.AGG, vertex["feature"], self.storage.part_id, aggregator_name=self.id)
                self.storage.message(query)

    def update_all_edges(self, new_vertex: "BaseVertex", old_vertex: "BaseVertex"):
        edge_list = self.storage.get_incident_edges(new_vertex, "both")  # In Edges, can do bulk update
        exchanges = []
        for edge in edge_list:
            try:
                if not edge.source.is_initialized or not edge.destination.is_initialized: continue
                if edge.destination == new_vertex:
                    msg_new = self.exchange(edge)
                    edge.destination = old_vertex
                    msg_old = self.exchange(edge)
                    exchanges.append((msg_new, msg_old))
                else:
                    msg_new = self.exchange(edge)
                    edge.source = old_vertex
                    msg_old = self.exchange(edge)
                    agg: "AggregatorFeatureMixin" = edge.destination['agg']
                    agg.replace(msg_new, msg_old)
            except KeyError:
                pass
        agg: "AggregatorFeatureMixin" = new_vertex['agg']
        agg.bulk_replace(*exchanges)

    def reduce_all_edges(self, vertex: "BaseVertex"):
        """ Bulk Reduce all in-edges and individual reduce for out-edges """
        in_edge_list = self.storage.get_incident_edges(vertex, "both")  # In Edges, can do bulk update
        exchanges = []
        for edge in in_edge_list:
            try:
                if not edge.source.is_initialized or not edge.destination.is_initialized: continue
                msg = self.exchange(edge)
                if edge.destination == vertex:
                    exchanges.append(msg)
                else:
                    agg: "AggregatorFeatureMixin" = edge.destination['agg']
                    agg.reduce(msg)
            except KeyError:
                pass
        agg: "AggregatorFeatureMixin" = vertex['agg']
        agg.bulk_reduce(*exchanges)


class StreamingGNNInference(BaseStreamingGNNInference):
    def __init__(self, *args, **kwargs):
        super(StreamingGNNInference, self).__init__(*args, **kwargs)
        self.message_fn = None
        self.update_fn = None

    def exchange(self, edge: "SimpleEdge") -> torch.tensor:
        """ Create the aggregator function and reduce the aggregator function """
        source: "SimpleVertex" = edge.source
        dest: "SimpleVertex" = edge.destination
        source_f = source['feature']
        destination_f = dest['feature']
        concat_f = torch.concat((source_f.value, destination_f.value), dim=0)
        msg = self.message_fn(concat_f)
        return msg

    def apply(self, vertex: "SimpleVertex") -> torch.tensor:
        feature = vertex['feature']
        agg = vertex['agg']
        conc = torch.concat((feature.value, agg.value[0]), dim=0)
        return self.update_fn(conc)

    def open(self, *args, **kwargs):
        self.message_fn = torch.nn.Linear(14, 32, dtype=torch.float32, bias=False)
        self.update_fn = torch.nn.Sequential(
            torch.nn.Linear(39, 16, bias=False),
            torch.nn.ReLU(),
            torch.nn.Linear(16, 7)
        )
