import os

import dgl
import argparse as arg
import torch as pt
from torch.distributed import init_process_group
import pandas as pd
from os.path import abspath, join



class TemporalEdgeSampler(dgl.dataloading.NeighborSampler):
    """ Sampler for edges with temporal ids """

    def __init__(self, num_layers=None, sample_size=None, **kwargs):
        assert num_layers is not None or sample_size is not None, "Either sample sample or num_layers should be not None"
        self.T = 0
        if num_layers is not None:
            super().__init__([-1] * num_layers, **kwargs)
        else:
            super().__init__(sample_size, **kwargs)

    def update_T(self, new_T):
        """ Call this method to update the time """
        self.T = new_T

    def filter_timestamp(self, edge):
        """ Filter function for edges according to timestamp """
        return edge.data["_ID"] > self.T

    def sample_neighbors(self, graph, seed_nodes, fanout, edge_dir='in', prob=None,
                         exclude_edges=None, replace=False, etype_sorted=True,
                         output_device=None):
        """ Changed the method for sampling because the default one did not fetch the out edges """
        if len(graph.etypes) > 1:
            frontier = dgl.distributed.graph_services.sample_etype_neighbors(
                graph, seed_nodes, dgl.distributed.graph_services.ETYPE, fanout, prob=prob, replace=replace,
                edge_dir=edge_dir, etype_sorted=etype_sorted)
        else:
            frontier = dgl.distributed.graph_services.sample_neighbors(
                graph, seed_nodes, fanout, replace=replace, prob=prob, edge_dir=edge_dir)
        return frontier

    def sample_out_nodes(self, g, seed_nodes, exclude_eids=None):
        """ Get the khop_out nodes as a list each element unique """
        output_nodes = list(seed_nodes)
        for fanout in reversed(self.fanouts[:-1]):
            if not len(seed_nodes):
                break
            frontier: dgl.DGLHeteroGraph = self.sample_neighbors(g,
                                                                 seed_nodes, fanout, edge_dir="out",
                                                                 prob=self.prob,
                                                                 replace=self.replace, output_device=self.output_device,
                                                                 exclude_edges=exclude_eids)
            frontier.remove_edges(frontier.filter_edges(self.filter_timestamp))
            seed_nodes = frontier.edges()[1]
            for i in seed_nodes:
                if i not in output_nodes:
                    output_nodes.append(i)
        return list(set(output_nodes))

    def sample_blocks(self, g, seed_nodes, exclude_eids=None):
        output_nodes = seed_nodes
        blocks = []
        for fanout in reversed(self.fanouts):
            frontier: dgl.DGLHeteroGraph = self.sample_neighbors(g,
                                                                 seed_nodes, fanout, edge_dir=self.edge_dir,
                                                                 prob=self.prob,
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
        NUM_LAYERS = 2
        graph = dgl.distributed.DistGraph(DATASET_NAME, None, join(PARTITION_DIR, DATASET_NAME + ".json"))
        local_graph: dgl.DGLHeteroGraph = graph._g
        sampler = TemporalEdgeSampler(num_layers=NUM_LAYERS, edge_dir="in")

        for i in range(local_graph.number_of_edges()):
            new_src, new_dest, time = local_graph.edges()[0][i], local_graph.edges()[1][i], \
                                      local_graph.edata["orig_id"][i]
            sampler.update_T(time)
            if time == 17386:
                print("")
            influenced_nodes = sampler.sample_out_nodes(graph, [new_dest])
            print("Time is %s, with %s of influenced nodes" % (time, len(influenced_nodes)))
            input_nodes, seed, blocks = sampler.sample(graph, influenced_nodes)
            features_batched = graph.ndata["features"][input_nodes]
            result = self.model(blocks, features_batched)
    def start_server(self):
        pass


# PART FOR PARTITIONING


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


def get_manual_graph() -> dgl.DGLGraph:
    graph = dgl.graph([[0, 1], [1, 2], [1, 3],[2, 4],[3, 4],[0, 5],[5, 4]])
    graph.ndata["features"] = pt.rand((6, 64))
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
        if DATASET_NAME == "manual-graph":
            graph = get_manual_graph()
        if graph is not None:
            partition_graph(graph, args.p)
        else:
            print("Dataset name is not recognized or error during parsing")
