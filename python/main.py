import logging
import sys
from datastream import GraphStream
from storage.gnn_layer import GNNLayerProcess
from partitioner import RandomPartitioner
from aggregator.gnn_layers_inference import StreamingGNNInference
from aggregator.output_gnn_prediction import MyOutputPrediction
from aggregator.output_gnn_training import MyOutputTraining
from helpers.streaming_train_splitter import StreamingTrainSplitter
from helpers.socketmapper import EdgeListParser


def run():
    graphstream = GraphStream(3)  # GraphStream with parallelism of 5
    graphstream.read_socket(EdgeListParser(
        ["Rule_Learning", "Neural_Networks", "Case_Based", "Genetic_Algorithms", "Theory", "Reinforcement_Learning",
         "Probabilistic_Methods"]), "localhost", 9090)  # Parse the incoming socket lines to GraphQueries
    graphstream.partition(RandomPartitioner())  # Partition the incoming GraphQueries to random partitions
    graphstream.train_test_split(StreamingTrainSplitter(0.2))
    graphstream.gnn_layer(
        GNNLayerProcess().with_aggregator(StreamingGNNInference(ident="rustam_streaming_gnn_inference")))
    graphstream.gnn_layer(
        GNNLayerProcess(is_last=True).with_aggregator(StreamingGNNInference(ident="rustam_streaming_gnn_inference")))
    graphstream.training_inference_layer(
        GNNLayerProcess().
            with_aggregator(MyOutputPrediction(ident="rustam_streaming_gnn_inference")).
            with_aggregator(MyOutputTraining(inference_name='rustam_streaming_gnn_inference'))
    )
    graphstream.last.print()
    print(graphstream.env.get_execution_plan())
    graphstream.env.execute("Test Python job")


if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        print(e)
