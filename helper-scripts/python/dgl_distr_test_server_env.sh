# Script for population necessary distributed dgl env variables for testing purposes
#!/bin/bash

export DGL_DIST_MODE=distributed
export DGL_IP_CONFIG=ip_config.txt
export DGL_GRAPH_FORMAT=csc
export DGL_KEEP_ALIVE=0
export DGL_DIST_MAX_TRY_TIMES=300
export DGL_NUM_SERVER=1
export DGL_DATASET_NAME=reddit-hyperlink
export DGL_CONF_PATH=reddit-hyperlink/reddit-hyperlink.json
export DGL_NUM_SAMPLER=1
export DGL_NUM_CLIENT=2
export DGL_ROLE=server

echo "Exported variables:"
echo "DGL_DIST_MODE=$DGL_DIST_MODE"
echo "DGL_IP_CONFIG=$DGL_IP_CONFIG"
echo "DGL_GRAPH_FORMAT=$DGL_GRAPH_FORMAT"
echo "DGL_KEEP_ALIVE=$DGL_KEEP_ALIVE"
echo "DGL_DIST_MAX_TRY_TIMES=$DGL_DIST_MAX_TRY_TIMES"
echo "DGL_NUM_SERVER=$DGL_NUM_SERVER"
echo "DGL_DATASET_NAME=$DGL_DATASET_NAME"
echo "DGL_CONF_PATH=$DGL_CONF_PATH"
echo "DGL_NUM_SAMPLER=$DGL_NUM_SAMPLER"
