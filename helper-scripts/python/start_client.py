""" Start Distributed DGL Clients """
import torch
import dgl
import os
import torch.nn as nn
import argparse as arg
import dgl.nn as dglnn
import time


# DGL_DIST_MODE=distributed DGL_IP_CONFIG=ip_config.txt DGL_GRAPH_FORMAT=csc DGL_KEEP_ALIVE=0 DGL_DIST_MAX_TRY_TIMES=300 DGL_NUM_SERVER=1 DGL_DATASET_NAME=reddit-hyperlink DGL_CONF_PATH=/home/rustambaku13/Documents/Warwick/flink-streaming-gnn/helper-scripts/python/reddit-hyperlink/reddit-hyperlink.json DGL_NUM_SAMPLER=1

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
        output_nodes = set(seed_nodes.tolist())
        for fanout in reversed(self.fanouts[:-1]):
            if not len(seed_nodes):
                break
            frontier: dgl.DGLHeteroGraph = self.sample_neighbors(g,
                                                                 seed_nodes, fanout, edge_dir="out",
                                                                 prob=self.prob,
                                                                 replace=self.replace, output_device=self.output_device,
                                                                 exclude_edges=exclude_eids)
            if frontier.number_of_edges():
                frontier.remove_edges(frontier.filter_edges(lambda a: g.edata["T"][frontier.edata["_ID"]] > self.T))
            seed_nodes = frontier.edges()[1]
            for i in seed_nodes.tolist():
                output_nodes.add(i)
        return torch.tensor(list(output_nodes), dtype=torch.int64)

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
            if frontier.number_of_edges():
                frontier.remove_edges(frontier.filter_edges(lambda a: g.edata["T"][frontier.edata["_ID"]] > self.T))
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
        self.layers.append(dglnn.SAGEConv(17, 128, 'mean'))
        self.layers.append(dglnn.SAGEConv(128, 64, 'mean'))
        self.layers.append(torch.nn.Linear(64, 32))
        self.layers.append(torch.nn.ReLU())
        self.layers.append(torch.nn.Linear(32, 16))
        self.layers.append(torch.nn.ReLU())
        self.layers.append(torch.nn.Linear(16,1))
        self.layers.append(torch.nn.Sigmoid())

    def forward(self, g, x):
        x_new = x
        for i, layer in enumerate(self.layers):
            if i >= 2:
                x_new = layer(x_new)
            else:
                x_new = layer(g[i], x_new)
        return x_new


if __name__ == '__main__':
    parser = arg.ArgumentParser(description='Partitioning some datasets')

    parser.add_argument('-W', type=int, required=False,
                        help='Window size')

    parser.add_argument('-T', type=bool, required=False, help="Training Script")

    args = parser.parse_args()

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

    pbook = g.get_partition_book()

    local_graph: dgl.DGLHeteroGraph = g.local_partition

    min_id = 0 if pbook.partid == 0 else pbook._max_edge_ids[pbook.partid - 1]

    max_id = pbook._max_edge_ids[pbook.partid]

    min_node_id = 0 if pbook.partid == 0 else pbook._max_node_ids[pbook.partid - 1]

    max_node_id = pbook._max_node_ids[pbook.partid]

    model = SAGE()

    sampler = TemporalEdgeSampler(num_layers=2, edge_dir="in")

    print("Starting Client on Part_id %s with %s edges" % (pbook.partid, (max_id - min_id)))
    if args.T:
        train_nid = dgl.distributed.node_split(torch.ones((g.number_of_nodes(),), dtype=torch.bool), pbook,
                                               force_even=True)
        print(len(train_nid), train_nid[0:10])
        train_nid = train_nid[g.ndata['labels'][train_nid] <= 1]
        print(len(train_nid), train_nid[0:10])
        model = torch.nn.parallel.DistributedDataParallel(model)
        sampler = dgl.dataloading.MultiLayerNeighborSampler([-1, -1])
        dataloader = dgl.dataloading.DistNodeDataLoader(
            g, train_nid, sampler, batch_size=50000,
            shuffle=True, drop_last=False)
        loss_fcn = nn.BCELoss()
        optimizer = torch.optim.Adam(model.parameters())
        start_training = int(round(time.time() * 1000))
        EPOCHS = 30
        for epoch in range(EPOCHS):
            for step, (input_nodes, seeds, blocks) in enumerate(dataloader):
                batch_inputs = g.ndata["features"][input_nodes]
                batch_labels = g.ndata["labels"][seeds]
                batch_pred = model(blocks, batch_inputs).flatten()
                correct = (batch_pred == batch_labels).sum().item()  # Compare and count correct predictions
                total = batch_labels.size(0)  # Total number of samples
                accuracy = correct / total  # Calculate accuracy
                loss = loss_fcn(batch_pred, batch_labels)
                optimizer.zero_grad()
                loss.backward()
                print(loss, accuracy)
                optimizer.step()

        training_time = int(round(time.time() * 1000)) - start_training
        print("Epochs Per Second:", EPOCHS / training_time * 1000)
        exit(1)

    elif args.W is None:
        for i in range(max_id - min_id):

            new_dest, T = local_graph.ndata["_ID"][local_graph.nodes() == local_graph.edges()[1][i]], g.edata["T"][local_graph.edata["_ID"][i]]

            sampler.update_T(T)

            influenced_nodes = sampler.sample_out_nodes(g, new_dest)

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
    else:
        W_local = args.W // g.get_partition_book().num_partitions()
        for i in range(0, (max_id - min_id - 1) // W_local + 1):
            if i == local_graph.number_of_edges() // W_local:
                new_dests, Ts = local_graph.ndata["_ID"][torch.isin(local_graph.nodes(), local_graph.edges()[1][
                                                                                         i * W_local:(
                                                                                                     max_id - min_id)])], \
                                g.edata["T"][local_graph.edata["_ID"][i * W_local:(max_id - min_id)]]
                if not len(new_dests):
                    break
            else:
                new_dests, Ts = local_graph.ndata["_ID"][torch.isin(local_graph.nodes(), local_graph.edges()[1][
                                                                                         i * W_local:(
                                                                                                                 i + 1) * W_local])], \
                                g.edata["T"][local_graph.edata["_ID"][i * W_local:(i + 1) * W_local]]

            sampler.update_T(Ts.max())

            influenced_nodes = sampler.sample_out_nodes(g, new_dests)

            input_nodes, seed, blocks = sampler.sample(g, influenced_nodes)

            features_batched = g.ndata["features"][input_nodes]

            result = model(blocks, features_batched)

            total_count += len(influenced_nodes)

            windowed_count += len(influenced_nodes)

            LOCAL_END = int(round(time.time() * 1000))

            print(i, TH_MEAN_LOCAL)

            if LOCAL_END - LOCAL_START > 1000:
                TH_MEAN_LOCAL = windowed_count / ((LOCAL_END - LOCAL_START)) * 1000
                if TH_MEAN_LOCAL > TH_MAX:
                    TH_MAX = TH_MEAN_LOCAL
                LOCAL_START = int(round(time.time() * 1000))
                windowed_count = 0

    END_TIME = int(round(time.time() * 1000))

    TH_MEAN = total_count / ((END_TIME - START_TIME)) * 1000

    print("Part_id: %s, Mean Throughput: %s\n Max Throughput: %s\nRuntime(s):%s Processed %s" % (
    pbook.partid, TH_MEAN, TH_MAX, (END_TIME - START_TIME) / 1000, i))
