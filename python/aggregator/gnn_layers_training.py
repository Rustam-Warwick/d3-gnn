import abc
from abc import ABCMeta
from typing import Sequence
import jax.numpy
from aggregator import BaseAggregator
from aggregator.gnn_output_inference import BaseStreamingOutputPrediction
from elements import GraphElement, GraphQuery, IterationState, RPCDestination
from decorators import rpc


class BaseStreamingLayerTraining(BaseAggregator, metaclass=ABCMeta):
    """ Base Class for GNN Final Layer when the predictions happen """

    def __init__(self, inference_agg: "BaseStreamingOutputPrediction", epochs=5, *args, **kwags):
        super(BaseStreamingLayerTraining, self).__init__(element_id="trainer")
        self.inference_agg: "BaseStreamingOutputPrediction" = inference_agg  # Reference to inference. Created on open()
        self.epochs = epochs  # Number of epochs on batch of training should go

    @abc.abstractmethod
    def backward(self, vertex_ids: Sequence[str], grad_vector: jax.numpy.array):
        """ Since this is the starting point, and it is being sent backwards no implementation needed for this """
        pass

    def run(self, query: "GraphQuery", **kwargs):
        if query.iteration_state is IterationState.BACKWARD:
            # Information comes from next layer essentially backprop
            print("This should not happen")
            # vertex_ids = query.element['vertex_ids']
            # vertices = list(map(lambda x: self.storage.get_vertex(x), vertex_ids))
            # vector_grads = query.element["grad_vector"]
            # self.backward(vertices, vector_grads)


class StreamingLayerTrainingJAX(BaseStreamingLayerTraining):
    def update_fn(self, update_params, agg, feature):
        conc = jax.numpy.concatenate((feature, agg))
        return self.inference_agg.update_fn.apply(update_params, conc)

    def message_fn(self, message_params, feature):
        return self.inference_agg.message_fn.apply(message_params, feature)

    @rpc(is_procedure=True, iteration=IterationState.ITERATE, destination=RPCDestination.CUSTOM)
    def msg_backward(self, vertex_ids: Sequence[str], msg_grad: jax.numpy.array):
        vertices = list(map(lambda x: self.storage.get_vertex(x), vertex_ids))
        msg_grads = []  # Message Param Grads
        source_vertices = []  # Source Vertices
        source_vertex_grads = None  # Grads of source vertices
        for i, vertex in enumerate(vertices):
            in_edges = self.storage.get_incident_edges(vertex, "in")
            in_edges = list(
                filter(lambda edge: edge.source.get('feature'), in_edges)) # Pick edges which have features
            if len(in_edges) == 0: continue
            in_features = jax.numpy.vstack([e.source["feature"].value for e in in_edges])
            loss, grad_fn = jax.vjp(jax.vmap(self.message_fn, [None, 0]),
                                    self.inference_agg['message_params'].value, in_features)
            message_grad, in_feature_grad = grad_fn(jax.numpy.tile(msg_grad[i], (len(in_edges), 1)))
            msg_grads.append(message_grad)
            source_vertices.extend([e.source for e in in_edges])
            if source_vertex_grads is None:
                source_vertex_grads = in_feature_grad
            else:
                source_vertex_grads = jax.numpy.concatenate((source_vertex_grads, in_feature_grad), axis=0)
        self.inference_agg['message_params'].batch_update(*msg_grads)  # Update message layer params
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
            pass

    @rpc(is_procedure=True, iteration=IterationState.BACKWARD, destination=RPCDestination.CUSTOM)
    def backward(self, vertex_ids: Sequence[str], grad_vector: jax.numpy.array):
        vertices = list(map(lambda x: self.storage.get_vertex(x), vertex_ids))
        batch_aggregations = jax.numpy.vstack(list(map(lambda x: x['agg'].value[0], vertices)))
        batch_features = jax.numpy.vstack(list(map(lambda x: x['feature'].value, vertices)))
        loss, grad_fn = jax.vjp(jax.vmap(self.update_fn, (None, 0, 0)), self.inference_agg['update_params'].value,
                                batch_aggregations, batch_features)
        update_fn_grads, agg_grad, feature_grad = grad_fn(grad_vector)

        msg_grad = jax.numpy.vstack([vertex['agg'].grad(agg_grad[ind]) for ind, vertex in enumerate(vertices)])
        # 2. Send msg_backward RPC to all replicas
        edge_part_dict = dict()
        for ind, vertex in enumerate(vertices):
            for i in vertex.replica_parts:
                if i not in edge_part_dict:
                    edge_part_dict[i] = [[vertex.id], msg_grad[ind, None]]
                else:
                    edge_part_dict[i][0].append(vertex.id)
                    edge_part_dict[i][1] = jax.numpy.vstack(edge_part_dict[i][1], msg_grad[ind, None])
        for part, params in edge_part_dict.items():
            self.msg_backward(*params, __parts=[part])
        self.msg_backward(vertex_ids, msg_grad, __call=True)  # Call directly master parts here
        self.inference_agg['update_params'].update(update_fn_grads)  # Apply the updates to update model parameters
        self.backward(vertex_ids, feature_grad, __parts=[self.part_id])
