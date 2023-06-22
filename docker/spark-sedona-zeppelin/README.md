# sedona_zeppelin-0.10.1
Image for apache sedona with Zeppelin and 1 spark master node and 1 worker node. The image is build on top of spark-3.0.1 and sedona-1.3.1-incubating as zeppelin latest version 0.10.1 doesn't support spark versions above 3.1.* and sedona 1.3.1-incubating is  compatible with spark 3.0.*.

# Docker Image
kartikeyhadiya/sedona_zeppelin-0.10.1:1.3.1-incubating

The image contains all the dependencies required for configuring Apache Sedona and Zeppelin. Zeppelin can be accessed at http://localhost:8082/ after pulling image.
May require enabling for helium plugin in zeppelin.

# Spark Configuration
  - Master node can be accessed at port http://localhost:8080/ (spark://spark-master:7077).
  - Spark-worker-1 can be accessed at port http://localhost:8081/.
  
# How to build
  - Clone the repository
  - Run docker-compose up

# Requirements
  - Docker
  - Docker-compose
