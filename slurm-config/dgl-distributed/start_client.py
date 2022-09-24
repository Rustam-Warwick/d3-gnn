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
    dgl.distributed.initialize(os.getenv("DGL_IP_CONFIG")

    torch.distributed.init_process_group("gloo")

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