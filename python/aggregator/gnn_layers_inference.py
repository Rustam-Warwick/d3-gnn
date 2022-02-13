import abc

import jax
from decorators import rpc
from aggregator import BaseAggregator
from elements import ElementTypes, GraphQuery, Op, ReplicaState, IterationState, RPCDestination
from elements.element_feature.aggregator_feature import JACMeanAggregatorReplicableFeature, AggregatorFeatureMixin
from elements.element_feature.tensor_feature import TensorReplicableFeature
from elements.element_feature.jax_params import JaxParamsFeature
from abc import ABCMeta
from flax.linen import Module
import jax.numpy as jnp
from elements.edge import BaseEdge
from elements.element_feature import ReplicableFeature
from elements.vertex import BaseVertex
from elements import GraphElement


class BaseStreamingGNNInference(BaseAggregator, metaclass=ABCMeta):
    """ Base class for stream gnn inference. Represents the embedding(L) Layers of GNN Aggregation @param
    layers_covered represents the depth of how many layers is this GNN Inference covering: Ex: If layers_covered = 2,
    it means that Layer=0, & and Layer=1 are going to happen in this horizontal operator and then pushed to next ones
    """

    def __init__(self, layers_covered: int = 1, *args, **kwargs):
        super(BaseStreamingGNNInference, self).__init__(*args, **kwargs)
        self.layers_covered = layers_covered

    @abc.abstractmethod
    def message(self, source_feature:jnp.array):
        pass

    @abc.abstractmethod
    def update(self, source_feature:jnp.array, agg:jnp.array):
        pass

    def run(self, query: "GraphQuery", *args, **kwargs):
        """ Take in the incoming aggregation result and add it to the storage engine, rest is in callbacks """
        el = self.storage.get_element(query.element)
        el.external_update(query.element)

    @rpc(is_procedure=True, destination=RPCDestination.SELF, iteration=IterationState.FORWARD)
    def init_vertex(self, vertex_id: str, agg: jnp.array, feature: jnp.array):
        vertex = self.storage.get_vertex(vertex_id)
        vertex['feature'] = TensorReplicableFeature(
            value=feature)
        vertex['agg'] = JACMeanAggregatorReplicableFeature(tensor=agg, is_halo=True)

        self.reduce_all_edges(vertex_id, __parts=[vertex.master_part, *vertex.replica_parts])  # Callback for
        # reducing all edges
        if not self.storage.is_last:
            next_feature = self.update(feature, agg)
            self.init_vertex(vertex_id, agg, next_feature)

    def add_element_callback(self, element: "GraphElement"):
        super(BaseStreamingGNNInference, self).add_element_callback(element)
        if self.storage.is_first:
            if element.element_type is ElementTypes.VERTEX and element.state is ReplicaState.MASTER:
                self.init_vertex(element.id, jnp.zeros((32,), dtype=jnp.float32),
                                 jnp.zeros((7,), dtype=jnp.float32),
                                 __call=True)  # Call here

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        super(BaseStreamingGNNInference, self).update_element_callback(element, old_element)
        if element.element_type is ElementTypes.FEATURE:
            element: "ReplicableFeature"
            old_element: "ReplicableFeature"
            if element.field_name == 'feature':
                # Genuine update on feature, update all the previous aggregations
                self.update_out_edges(element, old_element.value)
                if element.state is ReplicaState.MASTER:
                    embedding = self.update(element.element['feature'].value, element.element['agg'].value[0])
                    vertex = BaseVertex(master=element.element.master_part)
                    vertex.id = element.element.id
                    vertex["feature"] = TensorReplicableFeature(value=embedding)
                    query = GraphQuery(Op.AGG, vertex, vertex.master_part, aggregator_name=self.id)
                    self.storage.message(query)
            if element.field_name == 'agg' and element.state is not ReplicaState.MASTER:
                print("Not Master but still update why ??? ")
            if element.field_name == 'agg' and element.state is ReplicaState.MASTER:
                # Generate new embedding for the node & send to master part of the next layer
                embedding = self.update(element.element['feature'].value, element.element['agg'].value[0])
                vertex = BaseVertex(master=element.element.master_part)
                vertex.id = element.element.id
                vertex["feature"] = TensorReplicableFeature(value=embedding)
                query = GraphQuery(Op.AGG, vertex, vertex.master_part, aggregator_name=self.id)
                self.storage.message(query)

    def update_out_edges(self, vertex: "BaseVertex", old_feature: jnp.array):
        """ Updates the aggregations using all incident edges """
        edge_list = self.storage.get_incident_edges(vertex, edge_type="out")  # In Edges, can do bulk update
        msg_old = self.message(old_feature)
        for edge in edge_list:
            try:
                if not edge.source.get("feature") or not edge.destination.get("agg"): continue
                msg_new = self.message(edge.source['feature'].value)
                agg: "AggregatorFeatureMixin" = edge.destination['agg']
                agg.replace(msg_new, msg_old)
            except KeyError:
                pass

    @rpc(is_procedure=True, destination=RPCDestination.CUSTOM, iteration=IterationState.ITERATE)
    def reduce_all_edges(self, vertex_id: "BaseVertex"):
        """ Bulk Reduce all in-edges and individual reduce for out-edges """
        vertex = self.storage.get_vertex(vertex_id)
        in_edge_list = self.storage.get_incident_edges(vertex, "both")  # In Edges, can do bulk update
        exchanges = []
        for edge in in_edge_list:
            try:
                if not edge.source.get("feature") or not edge.destination.get("agg"): continue
                if edge.destination == vertex:
                    msg = self.message(edge.source['feature'].value)
                    exchanges.append(msg)
                else:
                    msg = self.message(edge.source['feature'].value)
                    agg: "AggregatorFeatureMixin" = edge.destination['agg']
                    agg.reduce(msg)
            except KeyError:
                pass

        agg: "AggregatorFeatureMixin" = vertex['agg']
        agg.bulk_reduce(*exchanges)


class StreamingGNNInferenceJAX(BaseStreamingGNNInference):
    def __init__(self, message_fn, update_fn, message_fn_params, update_fn_params, *args, **kwargs):
        super(StreamingGNNInferenceJAX, self).__init__(*args, **kwargs)
        self.message_fn: "Module" = message_fn
        self.update_fn: "Module" = update_fn
        self['message_params'] = JaxParamsFeature(value=message_fn_params)  # message params
        self['update_params'] = JaxParamsFeature(value=update_fn_params)  # update params

    def message(self, source_feature: jnp.array):
        return self.message_fn.apply(self['message_params'].value, source_feature)

    def update(self, source_feature: jnp.array, agg: jnp.array):
        conc = jnp.concatenate((source_feature, agg))
        return self.update_fn.apply(self['update_params'].value, conc)