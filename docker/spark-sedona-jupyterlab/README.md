# sedona_jupyterlab_spark
Dockerfiles for apache sedona with JupyterLab and 1 spark master node and 2 worker node

# Docker Image
kartikeyhadiya/sedona_jupyterlab:1.4.0

The image contains all the dependencies required for configuring Apache Sedona and JupyterLab. JupyterLab can be accessed at http://localhost:8888/ after pulling image.

# Spark Configuration
  - Master node can be accessed at port http://localhost:8080/ (spark://spark-master:7077). Refer link given in credit for more information.
  - Spark-worker-1 can be accessed at port http://localhost:8081/.
  - Spark-worker-2 can be accessed at port http://localhost:8082/.

# How to build
  - Clone the repository
  - Run build.sh script. (If using WSL, use dos2unix command to convert line endings to unix format)
  - Run docker-compose up

# Requirements
  - Docker
  - Docker-compose