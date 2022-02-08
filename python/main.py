from flax import linen as nn
from jax import lax, random, numpy as jnp
from flax.linen import relu, softmax, sigmoid
from nn.jax.multi_layer_dense import  MultiLayerDense
from datastream import GraphStream
from storage.gnn_layer import GNNLayerProcess
from partitioner import RandomPartitioner
from aggregator.gnn_layers_inference import StreamingGNNInferenceJAX
from aggregator.gnn_layers_training import StreamingLayerTrainingJAX
from aggregator.gnn_output_inference import StreamingOutputPredictionJAX
from aggregator.gnn_output_training import StreamingOutputTrainingJAX
from helpers.streaming_train_splitter import StreamingTrainSplitter
from helpers.socketmapper import EdgeListParser

def run():

    message_fn = MultiLayerDense(features=[32, 64, 32], activations=[relu, relu, relu])
    message_fn_params = message_fn.init(random.PRNGKey(0), random.uniform(random.PRNGKey(0), (14,)))
    update_fn = MultiLayerDense(features=[32, 16, 7], activations=[relu, relu, relu])
    update_fn_params = update_fn.init(random.PRNGKey(0), random.uniform(random.PRNGKey(0), (39,)))

    inferencer = StreamingGNNInferenceJAX(message_fn=message_fn,
                                          message_fn_params=message_fn_params,
                                          update_fn=update_fn,
                                          update_fn_params=update_fn_params
                                          )
    predict_fn = MultiLayerDense(features=[16, 32, 7], activations=[relu, relu, softmax ])
    predict_fn_params = predict_fn.init(random.PRNGKey(0), random.uniform(random.PRNGKey(0), (7,)))

    output_predictor = StreamingOutputPredictionJAX(
        predict_fn=predict_fn,
        predict_fn_params=predict_fn_params)

    graphstream = GraphStream(3)  # GraphStream with parallelism of 5
    graphstream.read_socket(EdgeListParser(
        ["Rule_Learning", "Neural_Networks", "Case_Based", "Genetic_Algorithms", "Theory", "Reinforcement_Learning",
         "Probabilistic_Methods"]), "localhost", 9090)  # Parse the incoming socket lines to GraphQueries
    graphstream.partition(RandomPartitioner())  # Partition the incoming GraphQueries to random partitions
    graphstream.train_test_split(StreamingTrainSplitter(0.2))

    graphstream.gnn_layer(
        GNNLayerProcess().with_aggregator(inferencer).with_aggregator(StreamingLayerTrainingJAX()))
    graphstream.gnn_layer(
        GNNLayerProcess(is_last=True).with_aggregator(inferencer).with_aggregator(StreamingLayerTrainingJAX()))

    graphstream.training_inference_layer(
        GNNLayerProcess().
            with_aggregator(output_predictor).
            with_aggregator(StreamingOutputTrainingJAX())
    )

    graphstream.last.print()
    print(graphstream.env.get_execution_plan())
    graphstream.env.execute("Test Python job")


if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        print(e)
