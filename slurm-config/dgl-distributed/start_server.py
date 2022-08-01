import dgl
import os

if __name__ == '__main__':
    print("Starting Server")
    tot_num_clients = 1 * (1 + 1) * int(os.environ["SLURM_NNODES"])
    os.environ.update({
        "DGL_DIST_MODE": "distributed",
        "DGL_ROLE": "server",
        "DGL_NUM_SAMPLER": "1",
        "OMP_NUM_THREADS": "1",
        "DGL_NUM_CLIENT": str(tot_num_clients),
        "DGL_CONF_PATH": "./stackoverflow_part/stackoverflow.json",
        "DGL_IP_CONFIG": "./ip_config_server.txt",
        "DGL_NUM_SERVER": "1",
        "DGL_GRAPH_FORMAT": "csc",
        "DGL_KEEP_ALIVE": "0",
        "DGL_DIST_MAX_TRY_TIMES": "300",
        "DGL_SERVER_ID": os.environ["SLURM_NODEID"]
        })
    dgl.distributed.initialize("./ip_config_server.txt") # Will wait here
    print("Server")