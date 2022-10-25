""" Script for emulating streaming uniEdges in DGL """
import torch
import pandas as pd
import dgl
import os
import torch.nn as nn
import dgl.nn as dglnn
import time


START_TIME = int(round(time.time() * 1000))
# Reddit dataset Load
data = pd.read_csv(os.path.join(os.environ["DATASET_DIR"], "RedditHyperlinks/soc-redditHyperlinks-body.tsv"), header=None, delimiter=",")

# Convert Node ids -> int
mapper = dict()
index = 0
src_ids = list()
dest_ids = list()
for i in range(len(data)):
    src = data.iloc[i][0]
    dest = data.iloc[i][1]
    if src not in mapper:
        mapper[src] = index
        index +=1
    if dest not in mapper:
        mapper[dest] = index
        index += 1
    src_ids.append(mapper[src])
    dest_ids.append(mapper[dest])

# Create DGL Graph
g = dgl.graph([])

g.add_nodes(len(mapper), {"x":torch.ones((len(mapper), 64))})

data = None # Garbage Collect

#Create 2-Layer GraphSage Model

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

model = SAGE()

# Run the job
total_count = 0
windowed_count = 0
LOCAL_START = int(round(time.time() * 1000))
TH_MAX = 0
TH_MEAN_LOCAL = None
for I in range(len(src_ids)):
    if I % 5000 == 0:
        print(I, TH_MEAN_LOCAL)
    g.add_edges([src_ids[I]], [dest_ids[I]])
    affected_nodes = g.khop_out_subgraph(dest_ids[I], 1)[0].ndata["_ID"]
    sampled_subgraph = g.khop_in_subgraph(affected_nodes, 2)[0]
    inf = model(sampled_subgraph, sampled_subgraph.ndata['x'])
    total_count += len(affected_nodes)
    windowed_count += len(affected_nodes)

    LOCAL_END = int(round(time.time() * 1000))
    if LOCAL_END - LOCAL_START > 1000:
        TH_MEAN_LOCAL = windowed_count / ((LOCAL_END - LOCAL_START)) * 1000
        if TH_MEAN_LOCAL > TH_MAX:
            TH_MAX = TH_MEAN_LOCAL
        LOCAL_START = int(round(time.time() * 1000))
        windowed_count = 0

# Print the runtime results

END_TIME = int(round(time.time() * 1000))

TH_MEAN = total_count / ((END_TIME - START_TIME)) * 1000

print("Mean Throughput: %s\n Max Throughput: %s\nRuntime(s):%s" % (TH_MEAN, TH_MAX, (END_TIME - START_TIME)/1000) )
