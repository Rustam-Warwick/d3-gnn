import abc

import jax

from exceptions import GraphElementNotFound
from decorators import rpc
from aggregator import BaseAggregator
from elements import ElementTypes, GraphQuery, Op, ReplicaState, IterationState, RPCDestination
from elements.element_feature.aggregator_feature import JACMeanAggregatorReplicableFeature, AggregatorFeatureMixin
from elements.element_feature.tensor_feature import TensorReplicableFeature
from elements.element_feature.jax_params import JaxParamsFeature
from abc import ABCMeta
from flax.linen import Module
import jax.numpy as jnp
from elements.element_feature import ReplicableFeature
from elements.vertex import BaseVertex
from elements.edge import BaseEdge
from elements import GraphElement


class BaseStreamingGNNInference(BaseAggregator, metaclass=ABCMeta):
    """ Base class for stream gnn inference. Represents the embedding(L) Layers of GNN Aggregation @param
    layers_covered represents the depth of how many layers is this GNN Inference covering: Ex: If layers_covered = 2,
    it means that Layer=0, & and Layer=1 are going to happen in this horizontal operator and then pushed to next ones
    """

    def __init__(self, layers_covered: int = 1, *args, **kwargs):
        super(BaseStreamingGNNInference, self).__init__(*args, **kwargs)

    @abc.abstractmethod
    def message(self, source_feature, *args, **kwargs):
        pass

    @abc.abstractmethod
    def update(self, source_feature, agg, *args, **kwargs):
        pass

    @rpc(is_procedure=True, destination=RPCDestination.SELF, iteration=IterationState.FORWARD)
    def forward(self, vertex_id, feature, part_id):
        try:
            vertex = self.storage.get_vertex(vertex_id)
            vertex['feature'].update_value(feature)  # Update value
        except GraphElementNotFound:
            vertex = BaseVertex(master=self.part_id)
            vertex.id = vertex_id
            vertex['feature'] = TensorReplicableFeature(value=feature)
            vertex.attach_storage(self.storage)
            vertex.create_element()

    @staticmethod
    def init_vertex(vertex):
        """ Initialize default aggregator and feature per vertex """
        vertex['agg'] = JACMeanAggregatorReplicableFeature(tensor=jnp.zeros((32,), dtype=jnp.float32), is_halo=True)
        vertex['feature'] = TensorReplicableFeature(
            value=jnp.zeros((7,), dtype=jnp.float32))

    def add_element_callback(self, element: "GraphElement"):
        super(BaseStreamingGNNInference, self).add_element_callback(element)
        if element.element_type is ElementTypes.VERTEX and element.state is ReplicaState.MASTER:
            self.init_vertex(element) # Initialize vertex features locally in the master part

        if element.element_type is ElementTypes.EDGE:
            element: "BaseEdge"
            if element.source.is_initialized and element.destination.is_initialized:
                agg: "AggregatorFeatureMixin" = element.destination['agg']
                msg = self.message(element.source['feature'].value, self['message_params'].value)
                agg.reduce(msg)

        if element.element_type is ElementTypes.FEATURE and (
                element.field_name == 'feature' or element.field_name == 'agg'):
            if element.element.get('agg') and element.element.get('feature'):
                self.reduce_all_edges(element.element.id, __call=True)  # Reduce all edges that were here before

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        super(BaseStreamingGNNInference, self).update_element_callback(element, old_element)
        if element.element_type is ElementTypes.FEATURE:
            element: "ReplicableFeature"
            old_element: "ReplicableFeature"
            if element.field_name == 'feature':
                # Genuine update on feature, update all the previous aggregations
                self.update_out_edges(element, old_element.value)
                if element.state is ReplicaState.MASTER:
                    if element.element['agg'].is_ready():
                        embedding = self.update(element.value, element.element['agg'].value[0], self['update_params'].value)
                        self.forward(element.element.id, embedding)
            if element.field_name == 'agg' and element.state is ReplicaState.MASTER:
                # Generate new embedding for the node & send to master part of the next laye
                if element.is_ready():
                    embedding = self.update(element.element['feature'].value, element.value[0], self['update_params'].value)
                    self.forward(element.element.id, embedding)
            if element.field_name == 'message_params':
                pass
                # @todo Implement new feature generation based on the updated model value

    def update_out_edges(self, vertex: "BaseVertex", old_feature: jnp.array):
        """ Updates the aggregations using all incident edges """
        edge_list = self.storage.get_incident_edges(vertex, edge_type="out")  # In Edges, can do bulk update
        msg_old = self.message(old_feature, self['message_params'].value)
        for edge in edge_list:
            try:
                if not edge.source.is_initialized or not edge.destination.is_initialized: continue
                msg_new = self.message(edge.source['feature'].value, self['message_params'].value)
                agg: "AggregatorFeatureMixin" = edge.destination['agg']
                agg.replace(msg_new, msg_old)
            except KeyError:
                pass

    @rpc(is_procedure=True, destination=RPCDestination.CUSTOM, iteration=IterationState.ITERATE)
    def reduce_all_edges(self, vertex_id: "BaseVertex", part_id):
        """ Bulk Reduce all in-edges and individual reduce for out-edges """
        vertex = self.storage.get_vertex(vertex_id)
        in_edge_list = self.storage.get_incident_edges(vertex, "both")  # In Edges, can do bulk update
        exchanges = []
        for edge in in_edge_list:
            try:
                if not edge.source.is_initialized or not edge.destination.is_initialized: continue
                if edge.destination == vertex:
                    msg = self.message(edge.source['feature'].value, self['message_params'].value)
                    exchanges.append(msg)
                else:
                    msg = self.message(edge.source['feature'].value, self['message_params'].value)
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

    def message(self, source_feature: jnp.array, params):
        return self.message_fn.apply(params, source_feature)

    def update(self, source_feature: jnp.array, agg: jnp.array, params):
        conc = jnp.concatenate((source_feature, agg))
        return self.update_fn.apply(params, conc)
