import os

import dgl
import argparse as arg
import torch as pt
from torch.distributed import init_process_group
import pandas as pd
from os.path import abspath, join


class TemporalEdgeSampler(dgl.dataloading.NeighborSampler):
    def __init__(self, num_layers=None, sample_size=None, **kwargs):
        assert num_layers is not None or sample_size is not None, "Either sample sample or num_layers should be not None"
        if num_layers is not None:
            super().__init__([-1] * num_layers, **kwargs)
        else:
            super().__init__(sample_size, **kwargs)

    def filter_timestamp(self, edge):
        return edge.data["_ID"] > 10


    def sample_blocks(self, g, seed_nodes, exclude_eids=None):
        output_nodes = seed_nodes
        blocks = []
        for fanout in reversed(self.fanouts):
            frontier:dgl.DGLHeteroGraph = g.sample_neighbors(
                seed_nodes, fanout, edge_dir=self.edge_dir, prob=self.prob,
                replace=self.replace, output_device=self.output_device,
                exclude_edges=exclude_eids)
            frontier.remove_edges(frontier.filter_edges(self.filter_timestamp))
            eid = frontier.edata[dgl.base.EID]
            block = dgl.to_block(frontier, seed_nodes)
            block.edata[dgl.base.EID] = eid
            seed_nodes = block.srcdata[dgl.base.NID]
            blocks.insert(0, block)

        return seed_nodes, output_nodes, blocks

class SAGE(pt.nn.Module):
    def __init__(self):
        super().__init__()
        self.layers = pt.nn.ModuleList()
        self.layers.append(dgl.nn.SAGEConv(64, 32, 'mean'))
        self.layers.append(dgl.nn.SAGEConv(32, 16, 'mean'))

    def forward(self, g, x):
        x_new = x
        for i, layer in enumerate(self.layers):
            x_new = layer(g[i], x_new)
        return x_new


class Tester:
    def __init__(self):
        self.model = SAGE()

    def start_client(self):
        os.environ["DGL_DIST_MODE"] = "standalone"
        with open("ip_config.txt", "w+") as f:
            f.write("localhost")
            dgl.distributed.initialize("ip_config.txt")
            # init_process_group("gloo", init_method="tcp://localhost:8000", world_size=1, rank=0)
            os.remove("ip_config.txt")
        graph = dgl.distributed.DistGraph(DATASET_NAME, None, join(PARTITION_DIR, DATASET_NAME+".json"))
        mask = dgl.distributed.node_split(pt.ones(graph.num_nodes(), dtype=pt.bool), graph.get_partition_book())
        train_dataloader = dgl.dataloading.DistNodeDataLoader(graph, mask, TemporalEdgeSampler(num_layers=2), batch_size=1024)

        for step, (input_nodes, seeds, blocks) in enumerate(train_dataloader):
            # Load the input features as well as output labels
            features_batched = graph.ndata["features"][input_nodes]
            result = self.model(blocks, features_batched)

            pass

    def start_server(self):
        pass

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
    parser.add_argument('--TEST', type=bool, required=False,
                        help='TEST MODE')

    args = parser.parse_args()
    DATASET_DIR = abspath(args.d)
    PARTITION_DIR = abspath(args.t)
    DATASET_NAME = args.n
    graph = None
    if args.TEST:
        tester = Tester()
        tester.start_client()
    else:
        if DATASET_NAME == "reddit-hyperlink":
            graph = get_reddit()

        if graph is not None:
            partition_graph(graph, args.p)
        else:
            print("Dataset name is not recognized or error during parsing")
