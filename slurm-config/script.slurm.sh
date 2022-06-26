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

FLINK_MEMORY_FRACTION=1 # percent of total memory allocated for flink

JOB_HEAP_FRACTION=0.5 # Job heap memory fraction

JOB_NATIVE_FRACTION=0.5 # Job native memory fraction

TASK_HEAP_FRACTION=0.4 # Task heap memory fraction

TASK_MANAGED_FRACTION=0 # Task Managed memory fraction

TASK_NATIVE_FRACTION=0.55 # Task native memory fraction

TOTAL_MEMORY=$(awk '/MemTotal/ {print $2}' /proc/meminfo) # Total memory of this machine
echo "$TOTAL_MEMORY"
TOTAL_FLINK_MEMORY=$("(${TOTAL_MEMORY} * ${FLINK_MEMORY_FRACTION}) / 1" | bc)

taskmanager_config() {
  export FLINK_CONF_DIR="${FLINK_HOME}"/conf-slurm-$SLURM_JOB_ID/"$(hostname)" # This worker Configuration directory
  mkdir -p "${FLINK_CONF_DIR}"
  cp -R "${FLINK_HOME}/template-slurm"/* "${FLINK_CONF_DIR}" # Copy master configurations to this conf directory
  TASK_HEAP_MEMORY=$("(${TOTAL_FLINK_MEMORY} * ${TASK_HEAP_FRACTION}) / 1" | bc)
  TASK_MANAGED_MEMORY=$("(${TOTAL_FLINK_MEMORY} * ${TASK_MANAGED_FRACTION}) / 1" | bc)
  TASK_NATIVE_MEMORY=$("(${TOTAL_FLINK_MEMORY} * ${TASK_NATIVE_FRACTION}) / 1" | bc)

}


jobmanager_config() {
  export FLINK_CONF_DIR="${FLINK_HOME}"/conf-slurm-$SLURM_JOB_ID/"$(hostname)" # This worker Configuration directory
  mkdir -p "${FLINK_CONF_DIR}"
  cp -R "${FLINK_HOME}/template-slurm"/* "${FLINK_CONF_DIR}" # Copy master configurations to this conf directory

}

if [[ -z "${SLURM_JOB_ID}" ]]; then
  echo "${TOTAL_MEMORY}"
else
  srun ./script.slurm
fi