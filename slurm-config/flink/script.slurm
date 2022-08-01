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

#SBATCH --job-name flink-slurm
#SBATCH --nodes=11
#SBATCH --cpus-per-task=40
#SBATCH --exclusive

NODE_MEMORY=$(scontrol show node $(hostname) | awk '/AllocMem/ {print $2}' | awk -F'=' '{print $2}') # Total memory available for this node

MEMORY_FRACTION=0.97 # Fraction of total memory used as Flink Total Memory. Leave some so nodes do not crash or freeze

JOBMANAGER_HEAP_FRACTION=0.8 # Fraction of Heap memory from the total memory for jobmanager

JOBMANAGER_OFF_HEAP_FRACTION=$(echo "1-$JOBMANAGER_HEAP_FRACTION" | bc) # Fraction of OFF_HEAP memory from the total memory for jobmanager

TASKMANAGER_HEAP_FRACTION=0.62 # Fraction of Heap memory from the total flink memory for taskmanager

TASKMANAGER_MANAGED_FRACTION=0.001 # Fraction Managed memory from the total flink memory for taskmanager

TASKMANAGER_OFF_HEAP_FRACTION=0.30 # Faction of Off Heap memory, Make the sum of Taskmanager configs < 1 since some needed for network memory

# Note that we are leaving 0.1 fraction of taskmanager for deafult flink memory config(Framework and network memory limits)



function master_configure {

	export FLINK_CONF_DIR="${FLINK_HOME}"/conf-slurm-$SLURM_JOB_ID/master # Master Configuration directory

	mkdir -p "${FLINK_CONF_DIR}" # Create

	cp -R "${FLINK_HOME}"/template-slurm/* "${FLINK_CONF_DIR}" # Copy current conf to master conf directory

	echo "$(hostname)" >>  "${FLINK_CONF_DIR}"/masters # Write hostname to masters file

	#NETWORK CONFIGURATION
	sed -i "/jobmanager\.rpc\.address/c\jobmanager.rpc.address: $(hostname)" "${FLINK_CONF_DIR}"/flink-conf.yaml # hostname

	sed -i "/rest\.address/c\rest.address: $(hostname)" "${FLINK_CONF_DIR}"/flink-conf.yaml # Rest endpoint

	# MEMORY CONFIGURATION
	JOBMANAGER_FLINK_MEMORY=$(echo "$NODE_MEMORY*$MEMORY_FRACTION/1" | bc)
	JOBMANAGER_HEAP_MEMORY=$(echo "$JOBMANAGER_FLINK_MEMORY*$JOBMANAGER_HEAP_FRACTION/1" | bc)
	JOBMANAGER_OFF_HEAP_MEMORY=$(echo "$JOBMANAGER_FLINK_MEMORY*$JOBMANAGER_OFF_HEAP_FRACTION/1" | bc)

	JOBMANAGER_CONFIG="jobmanager.memory.flink.size: ${JOBMANAGER_FLINK_MEMORY}m\njobmanager.memory.heap.size: ${JOBMANAGER_HEAP_MEMORY}m\njobmanager.memory.off-heap.size: ${JOBMANAGER_OFF_HEAP_MEMORY}m"
	sed -i "s/# JOBMANAGER_MEMORY.*/${JOBMANAGER_CONFIG}/g" "${FLINK_CONF_DIR}"/flink-conf.yaml
}


function replica_configure {

	export FLINK_CONF_DIR="${FLINK_HOME}"/conf-slurm-$SLURM_JOB_ID/"$(hostname)" # This worker

	mkdir -p "${FLINK_CONF_DIR}" # Create

	cp -R "${FLINK_HOME}"/template-slurm/* "${FLINK_CONF_DIR}" # Copy current conf to master conf directory

	# NETWORK CONFIGURATION

	sed -i "/jobmanager\.rpc\.address/c\jobmanager.rpc.address: ${MASTER_NODE} " "${FLINK_CONF_DIR}"/flink-conf.yaml # hostname

	sed -i "/rest\.address/c\rest.address: ${MASTER_NODE}" "${FLINK_CONF_DIR}"/flink-conf.yaml # Rest endpoint

	sed -i "/taskmanager\.host/c\taskmanager.host: $(hostname)" "${FLINK_CONF_DIR}"/flink-conf.yaml

	sed -i "/taskmanager\.numberOfTaskSlots/c\taskmanager.numberOfTaskSlots: $(($(nproc)))" "${FLINK_CONF_DIR}"/flink-conf.yaml


	#sed -i "/taskmanager\.numberOfTaskSlots/c\taskmanager.numberOfTaskSlots: 1" "${FLINK_CONF_DIR}"/flink-conf.yaml

	# MEMORY CONFIGURATION

	TASKMANAGER_FLINK_MEMORY=$(echo "$NODE_MEMORY*$MEMORY_FRACTION/1" | bc)
	TASKMANAGER_HEAP_MEMORY=$(echo "$TASKMANAGER_FLINK_MEMORY*$TASKMANAGER_HEAP_FRACTION/1" | bc)
	TASKMANAGER_MANAGED_MEMORY=$(echo "$TASKMANAGER_FLINK_MEMORY*$TASKMANAGER_MANAGED_FRACTION/1" | bc)
	TASKMANAGER_OFF_HEAP_MEMORY=$(echo "$TASKMANAGER_FLINK_MEMORY*$TASKMANAGER_OFF_HEAP_FRACTION/1" | bc)

	TASKMANAGER_CONFIG="taskmanager.memory.flink.size: ${TASKMANAGER_FLINK_MEMORY}m\ntaskmanager.memory.task.heap.size: ${TASKMANAGER_HEAP_MEMORY}m\ntaskmanager.memory.task.off-heap.size: ${TASKMANAGER_OFF_HEAP_MEMORY}m\ntaskmanager.memory.managed.size: ${TASKMANAGER_MANAGED_MEMORY}m\n"

	sed -i "s/# TASKMANAGER_MEMORY.*/${TASKMANAGER_CONFIG}/g" "${FLINK_CONF_DIR}"/flink-conf.yaml

}

if [ "$1" != 'run' ]; then
	srun --ntasks="${SLURM_NNODES}" --nodes="${SLURM_NNODES}" ~/script.slurm run # Run this script on all nodes and all tasks except for one. That is reserved for job submission. Only used for batch submission

else
    HOSTNAME=$(hostname)
    if [ "$SLURM_NODEID" -eq 0 ]; then
	# THIS IS THE MASTER NODE 0
	master_configure
	"${FLINK_HOME}"/bin/jobmanager.sh start-foreground  # Start the task manager in foreground mode
    else
	# THIS IS REPLICA NODE
	sleep 10 # Sleep to be sure that jobmanager is there
        MASTER_NODE=$( scontrol show hostname $SLURM_NODELIST | head -n 1 )
	replica_configure
	"${FLINK_HOME}"/bin/taskmanager.sh start-foreground  # Start the task manager in foreground mode

    fi
fi
