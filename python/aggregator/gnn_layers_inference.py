import abc

import jax

from exceptions import GraphElementNotFound
from decorators import rpc
from aggregator import BaseAggregator
from elements import ElementTypes, GraphQuery, Op, ReplicaState, IterationState, RPCDestination
from elements.element_feature.aggregator_feature import JACMeanAggregatorReplicableFeature, AggregatorFeatureMixin
from elements.element_feature.tensor_feature import VersionedTensorReplicableFeature
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

    def __init__(self, *args, **kwargs):
        super(BaseStreamingGNNInference, self).__init__(*args, **kwargs)

    @abc.abstractmethod
    def message(self, source_feature, *args, **kwargs):
        pass

    @abc.abstractmethod
    def update(self, source_feature, agg, *args, **kwargs):
        pass

    @rpc(is_procedure=True, destination=RPCDestination.SELF, iteration=IterationState.FORWARD)
    def forward(self, vertex_id, feature, part_id, part_version):
        if part_version < self.storage.part_version:
            # Previous is behind ignore this message
            # New embedding should come in soon
            pass
        else:
            vertex = self.storage.get_vertex(vertex_id)
            if vertex.get("feature"):
                # Feature exists
                vertex['feature'].version = part_version
                vertex['feature'].update_value(feature)  # Update value
            else:
                vertex['feature'] = VersionedTensorReplicableFeature(value=feature, version=part_version)

    def add_element_callback(self, element: "GraphElement"):
        super(BaseStreamingGNNInference, self).add_element_callback(element)
        if element.element_type is ElementTypes.VERTEX and element.state is ReplicaState.MASTER:
            element['agg'] = JACMeanAggregatorReplicableFeature(tensor=jnp.zeros((32,), dtype=jnp.float32),
                                                                is_halo=True)
            if element.storage.is_first:
                element['feature'] = VersionedTensorReplicableFeature(
                    value=jnp.zeros((7,), dtype=jnp.float32))

        if element.element_type is ElementTypes.EDGE:
            element: "BaseEdge"
            msg = self.get_message(element)
            if msg is not None:
                element.destination['agg'].reduce(msg)

        if element.element_type is ElementTypes.FEATURE and element.field_name == "feature":
            # For replicas on feature creation there might be some outstanding edges
            emb = self.get_update(element.element)
            if emb is not None:
                # @todo Add next layer agg here
                self.forward(element.element.id, emb)
            self.reduce_out_edges(element.element)
        if element.element_type is ElementTypes.FEATURE and element.field_name == "agg":
            # For replicas on feature creation there might be some outstanding edges
            self.reduce_in_edges(element.element)

    def update_element_callback(self, element: "GraphElement", old_element: "GraphElement"):
        super(BaseStreamingGNNInference, self).update_element_callback(element, old_element)
        if element.element_type is ElementTypes.FEATURE:
            element: "ReplicableFeature"
            old_element: "ReplicableFeature"
            if element.field_name == 'feature':
                # Update out-edges & create new embedding for next layer
                self.update_out_edges(element, old_element.value)
                emb = self.get_update(element.element)
                if emb is not None:
                    # @todo Add next layer agg here
                    self.forward(element.element.id, emb)

            if element.field_name == 'agg':
                # Generate new embedding for the node & send to master part of the next laye
                emb = self.get_update(element.element)
                if emb is not None:
                    # @todo Add next layer agg here
                    self.forward(element.element.id, emb)

            if element.field_name == 'params':
                # For first layer this start the computation For later layers this computes reduce for features which
                # arrived earlier than this model syced with previous version
                self.storage.part_version += 1
                vertices = self.storage.get_vertices()
                for vertex in vertices:
                    self.reduce_in_edges(vertex)

    def get_update(self, vertex: "BaseVertex"):
        if vertex.state is ReplicaState.MASTER and vertex.get("feature") and vertex.get("agg") and vertex[
            'agg'].is_ready and vertex['feature'].is_ready:
            embedding = self.update(vertex['feature'].value, vertex['agg'].value[0], self['params'].value[1])
            return embedding
        return None

    def get_message(self, edge: "BaseEdge"):
        if edge.destination.get("agg") and edge.source.get("feature") and edge.source['feature'].is_ready:
            msg = self.message(edge.source['feature'].value, self['params'].value[0])
            return msg
        return None

    def update_out_edges(self, vertex: "BaseVertex", old_feature: jnp.array):
        """ Updates the aggregations using all incident edges """
        edge_list = self.storage.get_incident_edges(vertex, edge_type="out")  # In Edges, can do bulk update
        msg_old = self.message(old_feature, self['params'].value[0])
        for edge in edge_list:
            try:
                msg_new = self.get_message(edge)
                if msg_new is None:
                    continue
                agg: "AggregatorFeatureMixin" = edge.destination['agg']
                agg.replace(msg_new, msg_old)
            except KeyError:
                pass

    def reduce_in_edges(self, vertex: "BaseVertex"):
        """ Updates the aggregations using all incident edges """
        if vertex.get('agg'):
            edge_list = self.storage.get_incident_edges(vertex, edge_type="in")  # In Edges, can do bulk update
            messages = []
            for edge in edge_list:
                try:
                    msg = self.get_message(edge)
                    if msg is None:
                        continue
                    messages.append(msg)
                except KeyError:
                    pass
            agg: "AggregatorFeatureMixin" = vertex['agg']
            agg.bulk_reduce(*messages)

    def reduce_out_edges(self, vertex: "BaseVertex"):
        """ Bulk Reduce all in-edges and individual reduce for out-edges """
        if vertex.get("feature"):
            edge_list = self.storage.get_incident_edges(vertex, "out")
            for edge in edge_list:
                try:
                    msg = self.get_message(edge)
                    if msg is None:
                        continue
                    agg: "AggregatorFeatureMixin" = edge.destination['agg']
                    agg.reduce(msg)
                except KeyError:
                    pass


class StreamingGNNInferenceJAX(BaseStreamingGNNInference):
    def __init__(self, message_fn, update_fn, message_fn_params, update_fn_params, *args, **kwargs):
        super(StreamingGNNInferenceJAX, self).__init__(*args, **kwargs)
        self.message_fn: "Module" = message_fn
        self.update_fn: "Module" = update_fn
        self['params'] = JaxParamsFeature(value=[message_fn_params, update_fn_params])  # message params
        # self['update_params'] = JaxParamsFeature(value=update_fn_params)  # update params

    def message(self, source_feature: jnp.array, params):
        return self.message_fn.apply(params, source_feature)

    def update(self, source_feature: jnp.array, agg: jnp.array, params):
        conc = jnp.concatenate((source_feature, agg))
        return self.update_fn.apply(params, conc)
