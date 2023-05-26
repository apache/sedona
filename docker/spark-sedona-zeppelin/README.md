# sedona_zeppelin-0.10.1
Image for apache sedona with Zeppelin and 1 spark master node and 1 worker node

# Docker Image
kartikeyhadiya/sedona_zeppelin-0.10.1:1.3.1-incubating

The image contains all the dependencies required for configuring Apache Sedona and Zeppelin. Zeppelin can be accessed at http://localhost:8082/ after pulling image.
May require enabling for helium plugin in zeppelin.

# Spark Configuration
  - Master node can be accessed at port http://localhost:8080/ (spark://spark-master:7077). Refer link given in credit for more information.
  - Spark-worker-1 can be accessed at port http://localhost:8081/.
  
# How to build
  - Clone the repository
  - Run docker-compose up

# Requirements
  - Docker
  - Docker-compose

# Credits
Spark image is based on https://github.com/big-data-europe/docker-spark and is maintained by bde2020
