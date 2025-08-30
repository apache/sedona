#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at

#   http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

DRIVER_MEM=${DRIVER_MEM:-4g}
EXECUTOR_MEM=${EXECUTOR_MEM:-4g}

# Function to convert memory string to megabytes
convert_to_mb() {
  local mem_str=$1
  local mem_value=${mem_str%[gGmM]}
  local mem_unit=${mem_str: -1}

  case $mem_unit in
    [gG])
      echo $(($mem_value * 1024))
      ;;
    [mM])
      echo "$mem_value"
      ;;
    *)
      echo "Invalid memory unit: $mem_str" >&2
      return 1
      ;;
  esac
}

# Convert DRIVER_MEM and EXECUTOR_MEM to megabytes
DRIVER_MEM_MB=$(convert_to_mb "$DRIVER_MEM")
if [ $? -ne 0 ]; then
  echo "Error converting DRIVER_MEM to megabytes." >&2
  exit 1
fi

EXECUTOR_MEM_MB=$(convert_to_mb "$EXECUTOR_MEM")
if [ $? -ne 0 ]; then
  echo "Error converting EXECUTOR_MEM to megabytes." >&2
  exit 1
fi

# Get total physical memory in megabytes
TOTAL_PHYSICAL_MEM_MB=$(free -m | awk '/^Mem:/{print $2}')

# Calculate the total required memory
TOTAL_REQUIRED_MEM_MB=$(($DRIVER_MEM_MB + $EXECUTOR_MEM_MB))

# Compare total required memory with total physical memory
if [ $TOTAL_REQUIRED_MEM_MB -gt "$TOTAL_PHYSICAL_MEM_MB" ]; then
    echo "Error: Insufficient memory" >&2
    echo "  total:    $TOTAL_PHYSICAL_MEM_MB MB" >&2
    echo "  required: $TOTAL_REQUIRED_MEM_MB MB (driver: $DRIVER_MEM_MB MB, executor: $EXECUTOR_MEM_MB MB)" >&2
    echo "Please tune DRIVER_MEM and EXECUTOR_MEM to smaller values." >&2
    echo "e.g: docker run -e DRIVER_MEM=2g -e EXECUTOR_MEM=2g ..." >&2
    exit 1
fi

# Configure Spark
cp "${SPARK_HOME}/conf/spark-env.sh.template" "${SPARK_HOME}/conf/spark-env.sh"

# Append Spark environment variables
{
  echo "SPARK_WORKER_MEMORY=${EXECUTOR_MEM}"
} >> "${SPARK_HOME}/conf/spark-env.sh"

# Append Spark default configurations
{
  echo "spark.driver.memory $DRIVER_MEM"
  echo "spark.executor.memory $EXECUTOR_MEM"
  echo "spark.sql.extensions org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions"
  echo "spark.hadoop.fs.s3a.aws.credentials.provider org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
} >> "${SPARK_HOME}/conf/spark-defaults.conf"

# Start spark standalone cluster
service ssh start
"${SPARK_HOME}"/sbin/start-all.sh

"${ZEPPELIN_HOME}"/bin/zeppelin-daemon.sh start
# Start jupyter lab
exec jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=
