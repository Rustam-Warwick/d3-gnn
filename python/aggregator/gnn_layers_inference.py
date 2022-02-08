import abc

import jax

from aggregator import BaseAggregator
from elements import ElementTypes, GraphQuery, Op, ReplicaState
from elements.element_feature.aggregator_feature import MeanAggregatorReplicableFeature, AggregatorFeatureMixin
from elements.element_feature.tensor_feature import TensorReplicableFeature
from elements.element_feature.jax_params import JaxParamsFeature
from copy import copy
from abc import ABCMeta
from flax.linen import Module
import jax.numpy as jnp
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

    def __init__(self, layers_covered: int = 1, ident: str = "streaming_gnn", storage: "GNNLayerProcess" = None):
        super(BaseStreamingGNNInference, self).__init__(ident, storage)
        self.layers_covered = layers_covered

    @abc.abstractmethod
    def message(self, feature):
        pass

    @abc.abstractmethod
    def update(self, feature):
        pass

    def run(self, query: "GraphQuery", *args, **kwargs):
        """ Take in the incoming aggregation result and add it to the storage engine, rest is in callbacks """
        el = self.storage.get_element(query.element)
        el.external_update(query.element)

    def add_element_callback(self, element: "GraphElement"):
        if element.element_type is ElementTypes.VERTEX and element.state is ReplicaState.MASTER:
            # Initialize the default tensor values, only on the MASTER node
            element['agg'] = MeanAggregatorReplicableFeature(
                tensor=jnp.zeros((32,), dtype=jnp.float32),
                is_halo=True)  # No need to replicate
            element['feature'] = TensorReplicableFeature(
                value=jnp.zeros((7,), dtype=jnp.float32))
            # Default is zero to have transitive relations
        if element.element_type is ElementTypes.EDGE:
            element: "BaseEdge"
            if element.source.is_initialized and element.destination.is_initialized:
                # If vertices are ready do the embedding now
                try:
                    msg = self.message(element)
                    agg: "AggregatorFeatureMixin" = element.destination['agg']
                    agg.reduce(msg)  # Update the aggregator
                except KeyError:
                    pass  # One of features is not here yet

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        if element.element_type is ElementTypes.VERTEX:
            if not old_element.is_initialized and element.is_initialized:
                # Bulk Reduce for all waiting nodes
                self.reduce_all_edges(element)
        if element.element_type is ElementTypes.FEATURE:
            element: "ReplicableFeature"
            old_element: "ReplicableFeature"
            if element.field_name == 'feature':
                # Genuine update on feature, update all the previous aggregations
                old_vertex = BaseVertex()
                old_vertex.id = element.element.id
                old_vertex["feature"] = old_element
                self.update_all_edges(element.element, old_vertex)

            if element.field_name == 'agg' and element.state is ReplicaState.MASTER:
                # Generate new embedding for the node & send to master part of the next layer
                embedding = self.update(element.element)
                vertex = BaseVertex(master=element.element.master_part)
                vertex.id = element.attached_to[1]
                vertex["feature"] = TensorReplicableFeature(value=embedding)
                query = GraphQuery(Op.AGG, vertex, vertex.master_part, aggregator_name=self.id)
                self.storage.message(query)

    def update_all_edges(self, new_vertex: "BaseVertex", old_vertex: "BaseVertex"):
        edge_list = self.storage.get_incident_edges(new_vertex, "both")  # In Edges, can do bulk update
        exchanges = []
        for edge in edge_list:
            try:
                if not edge.source.is_initialized or not edge.destination.is_initialized: continue
                if edge.destination == new_vertex:
                    msg_new = self.message(edge)
                    edge.destination = old_vertex
                    msg_old = self.message(edge)
                    exchanges.append((msg_new, msg_old))
                else:
                    msg_new = self.message(edge)
                    edge.source = old_vertex
                    msg_old = self.message(edge)
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
                msg = self.message(edge)
                if edge.destination == vertex:
                    exchanges.append(msg)
                else:
                    agg: "AggregatorFeatureMixin" = edge.destination['agg']
                    agg.reduce(msg)
            except KeyError:
                pass
        agg: "AggregatorFeatureMixin" = vertex['agg']
        agg.bulk_reduce(*exchanges)


class StreamingGNNInferenceJAX(BaseStreamingGNNInference):
    def __init__(self,  message_fn, update_fn, message_fn_params, update_fn_params, *args, **kwargs):
        super(StreamingGNNInferenceJAX, self).__init__(*args, **kwargs)
        self.message_fn: "Module" = message_fn
        self.update_fn: "Module" = update_fn
        self.message_fn_params = JaxParamsFeature(message_fn_params, master=0, element_id=self.id+"message") # message params
        self.update_fn_params = JaxParamsFeature(update_fn_params, master=0, element_id=self.id+"update")  # update params

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        super(StreamingGNNInferenceJAX, self).update_element_callback(element, old_element)
        if element.element_type is ElementTypes.FEATURE and element.field_name == self.id+"message":
            self.message_fn_params = element  # Update(cache) the old value
        if element.element_type is ElementTypes.FEATURE and element.field_name == self.id+"update":
            self.update_fn_params = element  # Update(cache) the old value

    @jax.jit
    def message(self, edge: "BaseEdge"):
        """ Return the embedding for the edge for this GNN Layer """
        source: "BaseVertex" = edge.source
        source_f = source['feature']
        return self.message_fn.apply(self.message_fn_params.value, source_f.value), jax.jacfwd(self.message_fn.apply)(self.message_fn_params.value, source_f.value)

    @jax.jit
    def update(self, vertex: "BaseVertex"):
        feature = vertex['feature']
        agg = vertex['agg']
        conc = jnp.concatenate((feature.value, agg.value[0]))
        return self.update_fn.apply(self.update_fn_params.value, conc)

    def open(self, *args, **kwargs):
        super().open(*args, **kwargs)
        self.message_fn_params.attach_storage(self.storage)
        self.message_fn_params.create_element()
        self.update_fn_params.attach_storage(self.storage)
        self.update_fn_params.create_element()


