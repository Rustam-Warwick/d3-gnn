""" Simple script for partitioning the dataset """
import dgl
import argparse as arg
import torch as pt
from datasets import EdgeListGraph
import os
import numpy as np


def partition_graph(graph: dgl.DGLGraph, num_parts: int, train_test_split=-1):
    if train_test_split > 0:
        # Do the train test split
        pass
    dgl.distributed.partition_graph(graph, graph_name=DATASET_NAME, num_parts=num_parts,
                                    out_path=os.path.join(PARTITION_DIR, DATASET_NAME),
                                    balance_edges=True)


def get_reddit() -> dgl.DGLGraph:
    graph = EdgeListGraph("RedditHyperlinks", "soc-redditHyperlinks-body", "tsv", "\t", usecols=range(2))
    graph.dataset[0] = graph.dataset[0].map(graph.nodes_2_index)
    graph.dataset[1] = graph.dataset[1].map(graph.nodes_2_index)
    dgl_graph = dgl.DGLGraph()
    dgl_graph.add_nodes(len(graph.nodes_2_index), data={"features": pt.rand((len(graph.nodes_2_index), 64))})
    dgl_graph.add_edges(graph.dataset[0].values, graph.dataset[1].values)
    dgl_graph.ndata["ID"] = pt.range(0, dgl_graph.number_of_nodes() - 1, dtype=pt.int)
    dgl_graph.edata["T"] = pt.range(0, dgl_graph.number_of_edges() - 1, dtype=pt.int)
    return dgl_graph


def get_dgraph_fin() -> dgl.DGLGraph:
    graph = EdgeListGraph("DGraphFin", "edge-list", "csv", ",", usecols=range(2))
    features = pt.tensor(np.load(os.path.join(os.environ["DATASET_DIR"], "DGraphFin", "node_features.npy")))
    labels = pt.tensor(np.load(os.path.join(os.environ["DATASET_DIR"], "DGraphFin", "node_labels.npy")))
    dgl_graph = dgl.DGLGraph()
    dgl_graph.add_nodes(len(graph.nodes_2_index), data={"features": features, "labels": labels})
    dgl_graph.add_edges(graph.dataset[0].values, graph.dataset[1].values)
    dgl_graph.ndata["ID"] = pt.range(0, dgl_graph.number_of_nodes() - 1, dtype=pt.int)
    dgl_graph.edata["T"] = pt.range(0, dgl_graph.number_of_edges() - 1, dtype=pt.int)
    return dgl_graph


def get_manual_graph() -> dgl.DGLGraph:
    graph = dgl.graph([[0, 1], [1, 2], [1, 3], [2, 4], [3, 4], [0, 5], [5, 4], [5, 7], [7, 8], [0, 4], [4, 8], [5, 6]])
    graph.ndata["ID"] = pt.range(0, graph.number_of_nodes() - 1)
    graph.ndata["features"] = pt.rand((graph.number_of_nodes(), 64))
    graph.edata["T"] = pt.range(0, graph.number_of_edges() - 1)
    return graph


DATASET_MAP = {
    "reddit-hyperlink": get_reddit,
    "manual-graph": get_manual_graph,
    "dgraph-fin": get_dgraph_fin
}

if __name__ == "__main__":
    global PARTITION_DIR, DATASET_NAME
    parser = arg.ArgumentParser(description='Partitioning some datasets')
    parser.add_argument('-p', type=int,
                        help='Number of parts', default=1)
    parser.add_argument('-n', type=str,
                        help='name of the dataset', default="reddit-hyperlink")
    parser.add_argument('-t', type=str,
                        help='directory where to partition to', default=".")
    args = parser.parse_args()
    PARTITION_DIR = os.path.abspath(args.t)
    DATASET_NAME = args.n
    graph = DATASET_MAP[DATASET_NAME]()
    if graph is not None:
        partition_graph(graph, args.p)
    else:
        print("Dataset name is not recognized or error during parsing")
