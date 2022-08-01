import torch
import pandas as pd
import dgl
import os
import torch.nn as nn
import torch.nn.functional as F
import dgl.nn as dglnn
import torch.optim as optim

class SAGE(nn.Module):
    def __init__(self):
        super().__init__()
        self.layers = nn.ModuleList()
        self.layers.append(dglnn.SAGEConv(64, 32, 'mean'))
        self.layers.append(dglnn.SAGEConv(32, 16, 'mean'))

    def forward(self, g, x):
        x_new = x
        for layer in self.layers:
            x_new = layer(g,x_new)
        return x_new


if __name__ == '__main__':

    print("Starting Client")

    tot_num_clients = 1 * (1 + 1) * int(os.environ["SLURM_NNODES"])

    os.environ.update({
        "DGL_DIST_MODE": "distributed",
        "DGL_ROLE": "client",
        "DGL_NUM_SAMPLER": "1",
        "DGL_NUM_CLIENT": str(tot_num_clients),
        "DGL_CONF_PATH": "./stackoverflow_part/stackoverflow.json",
        "DGL_IP_CONFIG": "./ip_config_server.txt",
        "DGL_NUM_SERVER": "1",
        "DGL_GRAPH_FORMAT": "csc",
        "OMP_NUM_THREADS": "4",
        "DGL_GROUP_ID": "0"
        })


    dgl.distributed.initialize("./ip_config_server.txt", num_worker_threads=4)

    print("Finished Initializae")

    torch.distributed.init_process_group("gloo")

    print("Started Client")

    g = dgl.distributed.DistGraph('stackoverflow')

    model = nn.parallel.DistributedDataParallel(SAGE())

    sampler = dgl.dataloading.MultiLayerFullNeighborSampler(2)

    inference_dataloader = dgl.dataloading.NodeDataLoader(
                             g, torch.ones(g.number_of_nodes()), sampler, batch_size=1024,
                             shuffle=True, drop_last=False)

    print("Ready to process data for %s vertices" % (g.number_of_nodes(), ))

    for step, (input_nodes, seeds, blocks) in enumerate(inference_dataloader):
        # Load the input features as well as output labels
        batch_inputs = g.ndata['x'][input_nodes]

        # Compute loss and prediction
        batch_pred = model(blocks, batch_inputs)
        print(step, "Finished")