import dgl
import argparse as arg
import numpy as np
import torch as pt
import pandas as pd
from os.path import abspath, join


def partition_graph(graph: dgl.DGLGraph, num_parts: int, train_test_split=-1):
    if train_test_split > 0:
        # Do the train test split
        pass
    dgl.distributed.partition_graph(graph, graph_name=DATASET_NAME, num_parts=num_parts,
                                    out_path=PARTITION_DIR,
                                    balance_edges=True)


def get_reddit() -> dgl.DGLGraph:
    data = pd.read_csv(join(DATASET_DIR, "soc-redditHyperlinks-body.tsv"), header=None, delimiter='\t',
                       usecols=range(2))  # Vertex, Vertex pairs
    vertex_names = pd.concat([data[0], data[1]], axis=0).unique().astype(str)
    vertex_id_map = {v: k for k, v in enumerate(vertex_names)}
    data[0] = data[0].map(vertex_id_map)
    data[1] = data[1].map(vertex_id_map)
    graph = dgl.DGLGraph()
    graph.add_nodes(vertex_names.shape[0], data={"features": pt.rand((vertex_names.shape[0], 64))})
    graph.add_edges(data[0].values, data[1].values)
    return graph


if __name__ == "__main__":

    global DATASET_DIR, PARTITION_DIR, DATASET_NAME
    parser = arg.ArgumentParser(description='Partitioning some datasets')

    parser.add_argument('-d', type=str,
                        help='directory of the dataset')
    parser.add_argument('-p', type=int,
                        help='Number of parts')
    parser.add_argument('-n', type=str,
                        help='name of the dataset')
    parser.add_argument('-t', type=str,
                        help='directory where to partition to')

    args = parser.parse_args()
    DATASET_DIR = abspath(args.d)
    PARTITION_DIR = abspath(args.t)
    DATASET_NAME = args.n
    graph = None
    if DATASET_NAME == "reddit-hyperlink":
        graph = get_reddit()

    if graph is not None:
        partition_graph(graph, args.p)
    else:
        print("Dataset name is not recognized or error during parsing")
