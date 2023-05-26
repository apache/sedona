# -- Software Stack Version

SPARK_VERSION="3.3.2"
HADOOP_VERSION="3"
# JUPYTERLAB_VERSION="2.1.5"

# -- Building the Images

docker build \
    -f base-jdk.dockerfile \
    -t base-jdk .

docker build \
    --build-arg spark_version="${SPARK_VERSION}" \
    --build-arg hadoop_version="${HADOOP_VERSION}" \
    -f spark-base.dockerfile \
    -t spark-base .

docker build \
    -f spark-master.dockerfile \
    -t spark-master .

docker build \
    -f spark-worker.dockerfile \
    -t spark-worker .

docker build \
    --build-arg spark_version="${SPARK_VERSION}" \
    --build-arg jupyterlab_version="${JUPYTERLAB_VERSION}" \
    -f jupyterlab.dockerfile \
    -t jupyterlab .