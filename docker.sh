docker build -t junhao/base-jdk -f docker/base-jdk.dockerfile  .
docker build -t kartikeyhadiya/spark-base:3.3.2 -f docker/spark-base.dockerfile  .
docker build -t kartikeyhadiya/spark-master -f docker/spark-master.dockerfile .
docker build -t kartikeyhadiya/spark-worker -f docker/spark-worker.dockerfile .
docker build -t kartikeyhadiya/sedona_jupyterlab -f docker/spark-sedona-jupyterlab/sedona_jupyterlab.dockerfile .