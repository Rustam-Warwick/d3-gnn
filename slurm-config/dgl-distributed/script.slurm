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
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=2
#SBATCH --exclusive

HOSTNAMES=$(scontrol show hostnames "$SLURM_NODELIST")

if [ "$1" != 'run' ]; then
	rm ip_config_server.txt
	touch ip_config_server.txt
	rm ip_config_client.txt
	touch ip_config_client.txt
	for HOSTNAME in $HOSTNAMES
	do
		echo "$HOSTNAME"
		echo "" >> ip_config_server.txt
		echo "" >> ip_config_client.txt
	done

        srun script.slurm run # Run the script in all the tasks

else
	index=$(($SLURM_PROCID % 2))
	HOSTNAME=$(hostname)
	if [ $index == 0 ]; then
		# This is server
		sleep $(($SLURM_NODEID + 1))

		[[ $(nslookup $(hostname)) =~ [0-9.]+$ ]]

		WRITE_VALUE_SERVER=$(printf "%ss/^/%s 11090/" $(($SLURM_NODEID + 1)) $BASH_REMATCH)

		sed -i "$WRITE_VALUE_SERVER" ip_config_server.txt

		python3.8 start_server.py

	else
		# This is client

		sleep 10

		[[ $(head -1 ip_config_server.txt) =~ [a-zA-Z0-9.-]+ ]]

		IP_MASTER=$BASH_REMATCH

		echo $IP_MASTER

		python3.8 -m torch.distributed.launch --nproc_per_node=1 --nnodes="$SLURM_NNODES" --node_rank="$SLURM_NODEID" --master_addr="$IP_MASTER" --master_port=11091 start_client.py
	fi
fi

wait