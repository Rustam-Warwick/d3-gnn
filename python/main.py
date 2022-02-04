import torch
from flax import linen as nn
from datastream import GraphStream
from storage.gnn_layer import GNNLayerProcess
from partitioner import RandomPartitioner
from aggregator.gnn_layers_inference import StreamingGNNInferenceJAX
from aggregator.output_gnn_prediction import StreamingOutputPrediction
from aggregator.output_gnn_training import StreamingOutputTraining
from helpers.streaming_train_splitter import StreamingTrainSplitter
from helpers.socketmapper import EdgeListParser


def run():
    inferencer = StreamingGNNInferenceJAX(ident="rustam_streaming_gnn_inference",
                                          message_fn=nn.Dense(features=32, use_bias=False),
                                          update_fn=nn.Dense(features=7, use_bias=False))

    output_predictor = StreamingOutputPrediction(ident='rustam_streaming_gnn_inference', predict_fn=torch.nn.Sequential(
        torch.nn.Linear(7, 32),
        torch.nn.Linear(32, 16),
        torch.nn.Linear(16, 7),
        torch.nn.Softmax(dim=0)
    ))

    graphstream = GraphStream(3)  # GraphStream with parallelism of 5
    graphstream.read_socket(EdgeListParser(
        ["Rule_Learning", "Neural_Networks", "Case_Based", "Genetic_Algorithms", "Theory", "Reinforcement_Learning",
         "Probabilistic_Methods"]), "localhost", 9090)  # Parse the incoming socket lines to GraphQueries
    graphstream.partition(RandomPartitioner())  # Partition the incoming GraphQueries to random partitions
    graphstream.train_test_split(StreamingTrainSplitter(0))
    graphstream.gnn_layer(
        GNNLayerProcess().with_aggregator(inferencer))
    graphstream.gnn_layer(
        GNNLayerProcess(is_last=True).with_aggregator(inferencer))
    graphstream.training_inference_layer(
        GNNLayerProcess().
            with_aggregator(output_predictor).
            with_aggregator(StreamingOutputTraining(inference_name='rustam_streaming_gnn_inference'))
    )
    graphstream.last.print()
    print(graphstream.env.get_execution_plan())
    graphstream.env.execute("Test Python job")


if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        print(e)
