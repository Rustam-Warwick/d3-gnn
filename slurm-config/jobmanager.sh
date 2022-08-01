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
#  limitations under the License.
################################################################################

# Need to have FLINK_HOME and FLINK_RUN_SCRIPT in the .env

#SBATCH --job-name flink-slurm
#SBATCH --nodes=4
#SBATCH --exclusive

USAGE="Usage: sbatch -p<PARTITION> -A<ACCOUNT> flink-slurm-example.sh"

if [[ -z $SLURM_JOB_ID ]]; then
    echo "No Slurm environment detected. $USAGE"
    exit 1
fi

# 1. Custom conf directory for this job
export FLINK_CONF_DIR="${FLINK_HOME}"/conf-slurm-$SLURM_JOB_ID # Configuration directory
mkdir -p "${FLINK_CONF_DIR}"/master
cp -R "${FLINK_HOME}"/conf/* "${FLINK_CONF_DIR}"/master # Copy current conf to master conf directory

# 2. Edit the configuration for jobmanager & start it

sed -i "/jobmanager\.rpc\.address/c\jobmanager.rpc.address: `hostname`" "${FLINK_CONF_DIR}"/master/flink-conf.yaml
"${FLINK_HOME}"/bin/jobmanager.sh start # Start the jobmanager on this Master machine

# 3. Start the worker nodes

WORKERS=()
for worker in `scontrol show hostnames`
do
  if [[ $worker != `hostname` ]]; then
    WORKERS+=("${worker}")
  fi
done

srun --nodes=1-1 --nodelist=${WORKERS} ${HOME}/taskmanager.sh # Start the task manager

# 4. Submit the job to master

# 5. Stop the taskmanager and jobmanagers

srun --nodes=1-1 --nodelist=${WORKERS} ${HOME}/taskmanager.sh # Start the task manager
# 6.
# Do we even need to populate those 2 files ?
# printf "%s\n" "`hostname`" > "${FLINK_CONF_DIR}/masters"
# printf "${WORKERS[@]}" > "${FLINK_CONF_DIR}/workers"

### Inspect nodes for CPU and memory and configure Flink accordingly ###

#echo
#echo "-----BEGIN FLINK CONFIG-----"
#

#
## 40 percent of available memory
#JOBMANAGER_HEAP=$(srun --nodes=1-1 --nodelist=$FLINK_MASTER awk '/MemTotal/ {printf( "%.2d\n", ($2 / 1024) * 0.4 )}' /proc/meminfo)
#sed -i "/jobmanager\.heap\.mb/c\jobmanager.heap.mb: $JOBMANAGER_HEAP" "${FLINK_CONF_DIR}"/flink-conf.yaml
#echo "jobmanager.heab.mb: $JOBMANAGER_HEAP"
#
## 80 percent of available memory
#echo "taskmanager.heap.mb: $TASKMANAGER_HEAP"
#
## number of phyical cores per task manager
#NUM_CORES=$(srun --nodes=1-1 --nodelist=$FLINK_MASTER cat /proc/cpuinfo | egrep "core id|physical id" | tr -d "\n" | sed s/physical/\\nphysical/g | grep -v ^$ | sort | uniq | wc -l)
#sed -i "/taskmanager\.numberOfTaskSlots/c\taskmanager.numberOfTaskSlots: $NUM_CORES" "${FLINK_CONF_DIR}"/flink-conf.yaml
#echo "taskmanager.numberOfTaskSlots: $NUM_CORES"
#
## number of nodes * number of physical cores
## PARALLELISM=$(cat $FLINK_HOME/conf/slaves | wc -l)
## PARALLELISM=$((PARALLELISM * NUM_CORES))
#PARALLELISM=1
#sed -i "/parallelism\.default/c\parallelism.default: $PARALLELISM" "${FLINK_CONF_DIR}"/flink-conf.yaml
#echo "parallelism.default: $PARALLELISM"
#
#echo "-----END FLINK CONFIG---"
#echo
#
#echo "Starting master on ${FLINK_MASTER} and slaves on ${FLINK_SLAVES[@]}."
#srun --nodes=1-1 --nodelist=${FLINK_MASTER} "${FLINK_HOME}"/bin/start-cluster
#sleep 120
#
#"${FLINK_HOME}"/bin/flink run "${FLINK_RUN_SCRIPT}"
#
#srun --nodes=1-1 --nodelist=${FLINK_MASTER} "${FLINK_HOME}"/bin/stop-cluster.sh
#sleep 120
#
#echo "Done."