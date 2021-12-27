from datastream import GraphStream
from partitioner import RandomPartitioner, KeySelector, Partitioner
from storage.map_storage import HashMapStorage
from partitioner import RandomPartitioner
from helpers.socketmapper import EdgeListParser


def run():
    graphstream = GraphStream(3)
    graphstream.read_file(EdgeListParser())
    graphstream.partition(RandomPartitioner())
    graphstream.storage(HashMapStorage())
    graphstream.env.execute("Test Python job")


if __name__ == '__main__':
    run()