import logging
import sys
from datastream import GraphStream
from storage.process_fn import GraphStorageProcess
from partitioner import RandomPartitioner
from aggregator.streaming_gnn_inference import StreamingGNNInference
from helpers.socketmapper import EdgeListParser

def run():
    graphstream = GraphStream(5)
    graphstream.read_socket(EdgeListParser(), "localhost", 9090)
    graphstream.partition(RandomPartitioner())
    graphstream.storage(GraphStorageProcess().with_aggregator(StreamingGNNInference(ident="rustam_streaming_gnn_inference")))
    graphstream.storage(GraphStorageProcess(is_last=True).with_aggregator(StreamingGNNInference(ident="rustam_streaming_gnn_inference")))
    graphstream.last.print()
    print(graphstream.env.get_execution_plan())
    graphstream.env.execute("Test Python job")


if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        print(e)

