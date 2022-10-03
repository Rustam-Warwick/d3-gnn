#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limittions under the License.
################################################################################

# Need to have FLINK_HOME in the .env

#SBATCH --job-name dgl-slurm
#SBATCH --nodes=7
#SBATCH --ntasks-per-node=2
#SBATCH --exclusive
#SBATCH --export=ALL,DGL_DIST_MODE=distributed,DGL_IP_CONFIG=ip_config.txt,DGL_GRAPH_FORMAT=csc,DGL_KEEP_ALIVE=0,DGL_DIST_MAX_TRY_TIMES=300,DGL_NUM_SERVER=1,DGL_DATASET_NAME=reddit-hyperlink,DGL_CONF_PATH=reddit-hyperlink/reddit-hyperlink.json,DGL_NUM_SAMPLER=1

# DGL_NUM_SERVER -> Number of server per machine, leave it 1. More than one means simply backup servers
# DGL_NUM_SAMPLER -> Number of samplers per client.

. $CONDA_ROOT/etc/profile.d/conda.sh

conda activate flink # flink the env for conda otherwise rename it

if [ "$1" != 'run' ]; then

	python3.8 dgl_dataset_partitioner.py  -p=${SLURM_NNODES} -d="${DATASET_DIR}/RedditHyperlink" -n="reddit-hyperlink" -t="./reddit-hyperlink" # Partition the dataset

	rm ip_config.txt

	touch ip_config.txt

	HOSTNAMES=$(scontrol show hostnames "$SLURM_NODELIST")

	for HOSTNAME in $HOSTNAMES

	do

		[[ $(nslookup $HOSTNAME) =~ [0-9.]+$ ]]

		echo "$(printf "%s 11090" $BASH_REMATCH)" >> ip_config.txt

	done

	srun script.slurm run # Run the script in all the tasks

else
	export DGL_NUM_CLIENT=$(( $DGL_NUM_SERVER * ( 1 + $DGL_NUM_SAMPLER ) * ( $( echo $SLURM_TASKS_PER_NODE | sed 's@^[^0-9]*\([0-9]\+\).*@\1@') - 1 ) * $SLURM_NNODES ))

	index=$(($SLURM_PROCID % $( echo $SLURM_TASKS_PER_NODE | sed 's@^[^0-9]*\([0-9]\+\).*@\1@')))

	if [ $index == 0 ]; then

		export DGL_ROLE=server

		export DGL_SERVER_ID=$SLURM_NODEID

		python3.8 -u -c "import dgl;import os;dgl.distributed.initialize(os.getenv(\"DGL_IP_CONFIG\"))" # Run server as a string script

	else
		# This is client
		export DGL_ROLE=client

		sleep 10

		[[ $(head -1 ip_config.txt) =~ [a-zA-Z0-9.-]+ ]] # First ip in the ip_config file is assumed to the Master address

		export MASTER_ADDR=$BASH_REMATCH

		export MASTER_PORT=11100

		export RANK=$SLURM_NODEID

		export WORLD_SIZE=$SLURM_NNODES

		python3.8 -u start_client.py
	fi
fi
