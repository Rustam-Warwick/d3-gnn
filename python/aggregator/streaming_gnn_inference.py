import abc
from copy import copy
from aggregator import BaseAggregator
from elements import ElementTypes, GraphQuery, Op, ReplicaState
from typing import TYPE_CHECKING
from elements.element_feature.tensor_feature import MeanAggregatorReplicableFeature, AggregatorFeatureMixin, \
    TensorReplicableFeature
import torch
from abc import ABCMeta

from elements.edge import BaseEdge
from elements.element_feature import ReplicableFeature
from elements.vertex import BaseVertex
from elements import GraphElement
from storage.process_fn import GraphStorageProcess


class BaseStreamingGNNInference(BaseAggregator, metaclass=ABCMeta):

    def __init__(self, minLevel=0, maxLevel=0, ident: str = "streaming_gnn", storage: "GraphStorageProcess" = None):
        super(BaseStreamingGNNInference, self).__init__(ident, storage)
        self.minLevel = minLevel
        self.maxLevel = maxLevel

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
            element['agg'] = MeanAggregatorReplicableFeature(tensor=torch.zeros((16, 16), dtype=torch.float32))

        if element.element_type is ElementTypes.EDGE:
            element: "BaseEdge"
            if element.source.is_initialized and element.destination.is_initialized:
                # If vertices are ready do the embedding now
                msg = self.exchange(element)
                agg: "AggregatorFeatureMixin" = element.destination['agg']
                agg.reduce(msg)  # Update the aggregator

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        if element.element_type is ElementTypes.VERTEX:
            if not old_element.is_initialized and element.is_initialized:
                # Bulk Reduce for all waiting nodes
                self.reduce_all_edges(element)
        if element.element_type is ElementTypes.FEATURE:
            element: "ReplicableFeature"
            old_element: "ReplicableFeature"
            if element.field_name == 'feature':
                # Do something new
                old_vertex = BaseVertex(element_id=element.element.id)
                old_vertex["feature"] = old_element
                self.update_all_edges(element.element, old_vertex)

            if element.field_name == 'agg':
                # Generate new embedding for the node & send to master part of the next layer
                embedding = self.apply(element.element)
                vertex = BaseVertex(element_id=element.element.id)
                vertex["feature"] = TensorReplicableFeature(value=embedding)
                query = None
                if element.state is ReplicaState.MASTER:
                    query = GraphQuery(Op.AGG, vertex["feature"], self.storage.part_id, aggregator_name=self.id)
                else:
                    query = GraphQuery(Op.AGG, vertex["feature"], element.master_part, aggregator_name=self.id)

                self.storage.message(query)

    def update_all_edges(self, new_vertex: "BaseVertex", old_vertex: "BaseVertex"):
        edge_list = self.storage.get_incident_edges(new_vertex, "both")  # In Edges, can do bulk update
        exchanges = []
        for edge in edge_list:
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
        agg: "AggregatorFeatureMixin" = new_vertex['agg']
        agg.bulk_replace(*exchanges)

    def reduce_all_edges(self, vertex: "BaseVertex"):
        """ Bulk Reduce all in-edges and individual reduce for out-edges """
        in_edge_list = self.storage.get_incident_edges(vertex, "both")  # In Edges, can do bulk update
        exchanges = []
        for edge in in_edge_list:
            if not edge.source.is_initialized or not edge.destination.is_initialized: continue
            msg = self.exchange(edge)
            if edge.destination == vertex:
                exchanges.append(msg)
            else:
                agg: "AggregatorFeatureMixin" = edge.destination['agg']
                agg.reduce(msg)
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
        with torch.no_grad():
            concat_f = torch.concat((source_f.value, destination_f.value), dim=1)
            msg = self.message_fn(concat_f)
        return msg

    def apply(self, vertex: "SimpleVertex") -> torch.tensor:
        feature = vertex['feature']
        agg = vertex['agg']
        with torch.no_grad():
            conc = torch.concat((feature.value, agg.value[0]), dim=1)
            return self.update_fn(conc)

    def open(self, *args, **kwargs):
        self.message_fn = torch.nn.Linear(32, 16, dtype=torch.float32, bias=False)
        self.update_fn = torch.nn.Sequential(
            torch.nn.Linear(32, 128),
            torch.nn.ReLU(),
            torch.nn.Linear(128, 32),
            torch.nn.ReLU(),
            torch.nn.Linear(32, 16)
        )
