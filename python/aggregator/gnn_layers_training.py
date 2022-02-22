import abc
from abc import ABCMeta
from typing import Sequence
import jax.numpy
from aggregator import BaseAggregator
from aggregator.gnn_layers_inference import BaseStreamingGNNInference
from elements import GraphElement, GraphQuery, IterationState, RPCDestination
from decorators import rpc


class BaseStreamingLayerTraining(BaseAggregator, metaclass=ABCMeta):
    """ Base Class for GNN Final Layer when the predictions happen """

    def __init__(self, inference_agg: "BaseStreamingOutputPrediction", epochs=5, *args, **kwags):
        super(BaseStreamingLayerTraining, self).__init__(element_id="trainer")
        self.inference_agg: "BaseStreamingGNNInference" = inference_agg  # Reference to inference. Created on open()
        self.epochs = epochs  # Number of epochs on batch of training should go
        self.update_grad_acc = None
        self.message_grad_acc = None
        self.update_grad_list = []
        self.message_grad_list = []
        self.msg_received = set()

    @rpc(is_procedure=True)
    def update_model(self, update_grad_acc, message_grad_acc, part_id):
        self.msg_received.add(part_id)
        self.update_grad_list.append(update_grad_acc)
        self.message_grad_list.append(message_grad_acc)

        if len(self.msg_received) == self.storage.parallelism:
            self.update_grad_list = list(filter(lambda x: x is not None, self.update_grad_list))
            self.message_grad_list = list(filter(lambda x: x is not None, self.message_grad_list))
            self.inference_agg['update_params'].batch_update(*self.update_grad_list)
            self.inference_agg['message_params'].batch_update(*self.message_grad_list)
            self.msg_received.clear()
            self.update_grad_list.clear()
            self.message_grad_list.clear()
    
    @abc.abstractmethod
    def backward(self, vertex_ids: Sequence[str], grad_vector: jax.numpy.array):
        """ Since this is the starting point, and it is being sent backwards no implementation needed for this """
        pass


class StreamingLayerTrainingJAX(BaseStreamingLayerTraining):
    def on_watermark(self):
        if self.storage.is_first:
            self.update_model(self.update_grad_acc, self.message_grad_acc)
            self.update_grad_acc = None
            self.message_grad_acc = None

    @rpc(is_procedure=True, iteration=IterationState.ITERATE, destination=RPCDestination.CUSTOM)
    def msg_backward(self, vertex_ids: Sequence[str], msg_grad: jax.numpy.array, part_id):
        """ Samples local in-edges of vertex_ids and backprops the message function """
        vertices = list(map(lambda x: self.storage.get_vertex(x), vertex_ids))
        msg_grads = []  # Message Param Grads
        source_vertices = []  # Source Vertices
        source_vertex_grads = None  # Grads of source vertices
        for i, vertex in enumerate(vertices):
            in_edges = self.storage.get_incident_edges(vertex, "in")
            in_edges = list(
                filter(lambda edge: edge.source.get('feature'), in_edges))  # Pick edges which have features
            if len(in_edges) == 0: continue
            in_features = jax.numpy.vstack([e.source["feature"].value for e in in_edges])
            loss, grad_fn = jax.vjp(jax.vmap(self.inference_agg.message, [0, None]),
                                    in_features, self.inference_agg['message_params'].value)
            message_grad, in_feature_grad = grad_fn(jax.numpy.tile(msg_grad[i], (len(in_edges), 1)))
            msg_grads.append(message_grad)
            source_vertices.extend([e.source for e in in_edges])
            if source_vertex_grads is None:
                source_vertex_grads = in_feature_grad
            else:
                source_vertex_grads = jax.numpy.concatenate((source_vertex_grads, in_feature_grad), axis=0)

        if self.message_grad_acc is None:
            self.message_grad_acc = jax.tree_multimap(lambda *x: jax.numpy.sum(jax.numpy.vstack(x), axis=0), *msg_grads)
        else:
            self.message_grad_acc = jax.tree_multimap(lambda *x: jax.numpy.sum(jax.numpy.vstack(x), axis=0),
                                                      self.message_grad_acc,
                                                      *msg_grads)
        # self.inference_agg['message_params'].batch_update(*msg_grads)  # Update message layer params
        if not self.storage.is_first:
            # If no more layer before this no need to send back
            source_part_dict = dict()
            for ind, vertex in enumerate(source_vertices):
                if vertex.master_part not in source_part_dict:
                    source_part_dict[vertex.master_part] = [[vertex.id], source_vertex_grads[ind, None]]
                else:
                    source_part_dict[vertex.master_part][0].append(vertex.id)
                    source_part_dict[vertex.master_part][1] = jax.numpy.concatenate(
                        (source_part_dict[vertex.master_part][1],
                         source_vertex_grads[ind, None]), axis=0)
            for part, params in source_part_dict.items():
                self.backward(*params, __parts=[part])
        else:
            # This is the last layer do new processing starting from here
            # If Feature values are learnable this is the part to it
            pass

    @rpc(is_procedure=True, iteration=IterationState.BACKWARD, destination=RPCDestination.CUSTOM)
    def backward(self, vertex_ids: Sequence[str], grad_vector: jax.numpy.array):
        vertices = list(map(lambda x: self.storage.get_vertex(x), vertex_ids))
        batch_aggregations = jax.numpy.vstack(list(map(lambda x: x['agg'].value[0], vertices)))
        batch_features = jax.numpy.vstack(list(map(lambda x: x['feature'].value, vertices)))
        loss, grad_fn = jax.vjp(jax.vmap(self.inference_agg.update, (0, 0, None)),
                                batch_aggregations, batch_features, self.inference_agg['update_params'].value)
        update_fn_grads, agg_grad, feature_grad = grad_fn(grad_vector)

        msg_grad = jax.numpy.vstack(
            [vertex['agg'].grad(agg_grad[ind]) for ind, vertex in enumerate(vertices)])  # dloss/dmessage
        # 2. Send msg_backward RPC to all replicas
        edge_part_dict = dict()
        for ind, vertex in enumerate(vertices):
            for i in vertex.replica_parts:
                if i not in edge_part_dict:
                    edge_part_dict[i] = [[vertex.id], msg_grad[ind, None]]
                else:
                    edge_part_dict[i][0].append(vertex.id)
                    edge_part_dict[i][1] = jax.numpy.vstack((edge_part_dict[i][1], msg_grad[ind, None]))
        for part, params in edge_part_dict.items():
            self.msg_backward(*params, __parts=[part])
        self.msg_backward(vertex_ids, msg_grad, __call=True)  # Call directly master parts here

        if self.update_grad_acc is None:
            self.update_grad_acc = update_fn_grads
        else:
            self.update_grad_acc = jax.tree_multimap(lambda *x: jax.numpy.sum(jax.numpy.vstack(x), axis=0),
                                                     self.update_grad_acc,
                                                     update_fn_grads)
        # self.inference_agg['update_params'].update(update_fn_grads)  # Apply the updates to update model parameters
        if not self.storage.is_first:
            self.backward(vertex_ids, feature_grad, __parts=[self.part_id])
