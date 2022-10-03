import torch
import pandas as pd
import dgl
import os
import torch.nn as nn
import torch.nn.functional as F
import dgl.nn as dglnn
import time
import torch.optim as optim


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
        """ Sample the graph as blocks """
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


class SAGE(nn.Module):
    def __init__(self):
        super().__init__()
        self.layers = nn.ModuleList()
        self.layers.append(dglnn.SAGEConv(64, 32, 'mean'))
        self.layers.append(dglnn.SAGEConv(32, 16, 'mean'))

    def forward(self, g, x):
        x_new = x
        for i, layer in enumerate(self.layers):
            x_new = layer(g[i], x_new)
        return x_new


if __name__ == '__main__':
    START_TIME = int(round(time.time() * 1000))

    total_count = 0

    windowed_count = 0

    LOCAL_START = int(round(time.time() * 1000))

    TH_MAX = 0

    TH_MEAN_LOCAL = None

    # Time related stuff finished
    dgl.distributed.initialize(os.getenv("DGL_IP_CONFIG"))

    torch.distributed.init_process_group("gloo")

    g = dgl.distributed.DistGraph(os.getenv("DGL_DATASET_NAME"))

    local_graph: dgl.DGLHeteroGraph = g._g

    model = SAGE()

    sampler = TemporalEdgeSampler(num_layers=2, edge_dir="in")

    for i in range(local_graph.number_of_edges()):
        new_src, new_dest, T = local_graph.edges()[0][i], local_graph.edges()[1][i], \
                                  local_graph.edata["_ID"][i] # Assuming data is shuffled

        src_index = (local_graph.nodes() == new_src)

        dest_index = (local_graph.nodes() == new_dest)

        new_src = local_graph.ndata["_ID"][src_index]

        new_dest = local_graph.ndata["_ID"][dest_index]

        sampler.update_T(T)

        influenced_nodes = sampler.sample_out_nodes(g, [new_dest])

        # print("Time is %s, with %s of influenced nodes and edge %s -> %s" % (
        # time, len(influenced_nodes), new_src, new_dest))

        input_nodes, seed, blocks = sampler.sample(g, influenced_nodes)

        features_batched = g.ndata["features"][input_nodes]

        result = model(blocks, features_batched)

        total_count += len(influenced_nodes)

        windowed_count += len(influenced_nodes)

        LOCAL_END = int(round(time.time() * 1000))
        if i % 5000 == 0:
            print(i, TH_MEAN_LOCAL)
        if LOCAL_END - LOCAL_START > 1000:
            TH_MEAN_LOCAL = windowed_count / ((LOCAL_END - LOCAL_START)) * 1000
            if TH_MEAN_LOCAL > TH_MAX:
                TH_MAX = TH_MEAN_LOCAL
            LOCAL_START = int(round(time.time() * 1000))
            windowed_count = 0

    END_TIME = int(round(time.time() * 1000))

    TH_MEAN = total_count / ((END_TIME - START_TIME)) * 1000

    print("Mean Throughput: %s\n Max Throughput: %s\nRuntime(s):%s" % (TH_MEAN, TH_MAX, (END_TIME - START_TIME) / 1000))