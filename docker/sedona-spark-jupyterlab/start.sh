#!/usr/bin/env bash

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
      echo $mem_value
      ;;
    *)
      echo "Invalid memory unit: $mem_str" >&2
      return 1
      ;;
  esac
}

# Convert DRIVER_MEM and EXECUTOR_MEM to megabytes
DRIVER_MEM_MB=$(convert_to_mb $DRIVER_MEM)
if [ $? -ne 0 ]; then
  echo "Error converting DRIVER_MEM to megabytes." >&2
  exit 1
fi

EXECUTOR_MEM_MB=$(convert_to_mb $EXECUTOR_MEM)
if [ $? -ne 0 ]; then
  echo "Error converting EXECUTOR_MEM to megabytes." >&2
  exit 1
fi

# Get total physical memory in megabytes
TOTAL_PHYSICAL_MEM_MB=$(free -m | awk '/^Mem:/{print $2}')

# Calculate the total required memory
TOTAL_REQUIRED_MEM_MB=$(($DRIVER_MEM_MB + $EXECUTOR_MEM_MB))

# Compare total required memory with total physical memory
if [ $TOTAL_REQUIRED_MEM_MB -gt $TOTAL_PHYSICAL_MEM_MB ]; then
    echo "Error: Insufficient memory" >&2
    echo "  total:    $TOTAL_PHYSICAL_MEM_MB MB" >&2
    echo "  required: $TOTAL_REQUIRED_MEM_MB MB (driver: $DRIVER_MEM_MB MB, executor: $EXECUTOR_MEM_MB MB)" >&2
    echo "Please tune DRIVER_MEM and EXECUTOR_MEM to smaller values." >&2
    echo "e.g: docker run -e DRIVER_MEM=2g -e EXECUTOR_MEM=2g ..." >&2
    exit 1
fi

# Configure spark
cp ${SPARK_HOME}/conf/spark-env.sh.template ${SPARK_HOME}/conf/spark-env.sh
echo "SPARK_WORKER_MEMORY=${EXECUTOR_MEM}" >> ${SPARK_HOME}/conf/spark-env.sh
echo "spark.driver.memory $DRIVER_MEM" >> ${SPARK_HOME}/conf/spark-defaults.conf
echo "spark.executor.memory $EXECUTOR_MEM" >> ${SPARK_HOME}/conf/spark-defaults.conf

# Start spark standalone cluster
service ssh start
${SPARK_HOME}/sbin/start-all.sh

# Start jupyter lab
exec jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=
