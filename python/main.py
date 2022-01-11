import logging
import sys
from datastream import GraphStream
from storage.process_fn import GraphStorageProcess
from partitioner import RandomPartitioner
from aggregator.streaming_gnn_inference import StreamingGNNInference
from helpers.socketmapper import EdgeListParser


def run():
    storage = GraphStorageProcess()
    # storage.with_aggregator
    graphstream = GraphStream(2)
    graphstream.read_socket(EdgeListParser(), "localhost", 9090)
    graphstream.partition(RandomPartitioner)
    graphstream.storage(storage)
    graphstream.last.print()
    graphstream.env.execute("Test Python job")


if __name__ == '__main__':
    try:

        run()
    except Exception as e:
        print(e)

