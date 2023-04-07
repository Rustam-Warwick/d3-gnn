import os
import pandas as pd
import numpy as np
import string
import random
from scipy.io import loadmat
import matplotlib.pyplot as plt
import metis

class EdgeListDataset:
    def __init__(self, folder_name, file_name, extension, sep, transform=None):
        self.folder_name = folder_name
        self.file_name = file_name
        self.extension = extension
        self.sep = sep
        self.dataset = pd.read_csv(os.path.join(os.environ["DATASET_DIR"], folder_name, f"{file_name}.{extension}"), header=None, sep=sep)
        if transform is not None:
            self.dataset = transform(self.dataset)
        nodes = pd.concat((self.dataset[0], self.dataset[1])).unique()
        self.nodes_2_index = dict(zip(nodes, range(len(nodes))))
        self.index_2_nodes = {v: k for k, v in self.nodes_2_index.items()}

        
    def partition(self, num_partitions):
        def get_num_parts(group):
            parts = set(group['parts'].unique())
            mas = self.master_parts[self.nodes_2_index[group[1].iloc[0]]]
            parts.add(mas)
            return len(parts)
        
        src_indices = self.dataset[0].map(self.nodes_2_index).values
        dest_indices = self.dataset[1].map(self.nodes_2_index).values
        adj_list = [[] for _ in range(len(self.nodes_2_index))]
        for i in range(len(src_indices)):
            found = False
            for j in adj_list[src_indices[i]]:
                if j[0] == dest_indices[i]:
                    j[1]+=1 # Weighted
                    found = True
                    break
            if not found:
                adj_list[src_indices[i]].append([dest_indices[i], 1])
        cut, self.master_parts = metis.part_graph(adj_list, num_partitions)
        self.dataset["parts"] = self.dataset[0].map(self.nodes_2_index).map(lambda x: self.master_parts[x])
        rf = (self.dataset.groupby(1).apply(get_num_parts) - 1).sum() / len(self.nodes_2_index)
        print(f"Partitioned to {num_partitions} partitions with cut {cut} and RF {rf}")
    
    def plot_degree(self, bins=200, log=False, deg_type="out"):
        if deg_type == "out": 
            degrees = self.dataset.groupby(0).apply(lambda x: x.shape[0]).values
        elif deg_type == "in":
            degrees = self.dataset.groupby(1).apply(lambda x: x.shape[0]).values
        plt.hist(degrees, bins=bins, density=True, alpha=0.5, log=log)
    
    def save_tsv(self):
        self.dataset.to_csv(os.path.join(os.environ["DATASET_DIR"], self.folder_name, f"{self.file_name}.tsv"),sep='\t', index=False, header=False)
    
class DGraphFin(EdgeListDataset):
    def __init__(self, folder_name, file_name):
        self.folder_name = folder_name
        self.file_name = file_name
        self.npzarr = np.load(os.path.join(os.environ["DATASET_DIR"], folder_name, f"{file_name}.npz"))
        order = self.npzarr['edge_timestamp'].argsort()
        self.dataset = pd.DataFrame(np.hstack((self.npzarr['edge_index'][order], self.npzarr['edge_timestamp'][order].reshape((1, -1)).T)))
        nodes = pd.concat((self.dataset[0], self.dataset[1])).unique()
        self.nodes_2_index = dict(zip(nodes, range(len(nodes))))
        self.index_2_nodes = {v: k for k, v in self.nodes_2_index.items()}
        
    def save_feature_and_labels(self):
        np.save(os.path.join(os.environ["DATASET_DIR"], self.folder_name, "node_features"), self.npzarr['x'].astype("float32"))
        np.save(os.path.join(os.environ["DATASET_DIR"], self.folder_name, "node_labels"), self.npzarr['y'].astype("int8"))