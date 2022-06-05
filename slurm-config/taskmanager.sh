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

if [[ $1 == "stop" ]]; then
    "${FLINK_HOME}"/bin/taskmanager.sh stop # Stop the taskmanager
    exit 0
fi

echo Taskmanager
# 1. Take the master configuration and replicate for taskmanager
export FLINK_MASTER_CONF="${FLINK_HOME}"/conf-slurm-$SLURM_JOB_ID/master # Master configuration directory
export FLINK_CONF_DIR="${FLINK_HOME}"/conf-slurm-$SLURM_JOB_ID/"$(hostname)" # This worker Configuration directory
mkdir -p "${FLINK_CONF_DIR}"
cp -R "${FLINK_MASTER_CONF}"/* "${FLINK_CONF_DIR}" # Copy master configurations to this conf directory

# 2. Taskmanager specific configurations
sed -i "/taskmanager\.rpc\.address/c\taskmanager.rpc.address: $(hostname)" "${FLINK_CONF_DIR}"/flink-conf.yaml
"${FLINK_HOME}"/bin/taskmanager.sh start # Start the jobmanager on this Master machine
