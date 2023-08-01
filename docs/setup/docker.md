# Sedona JupyterLab Docker Image

Dockerfiles for Apache Sedona with JupyterLab and 1 master node and 1 worker node

## How to use

### Pull the image from DockerHub

### Start the container

Format:

```bash
docker run -p 8888:8888 -p 8080:8080 -p 8081:8081 -p 4040:4040 sedona/sedona_jupyterlab:<sedona_version>
```

Example:

```bash
docker run -p 8888:8888 -p 8080:8080 -p 8081:8081 -p 4040:4040 sedona/sedona_jupyterlab:latest
```

This command will bind the container's ports 8888, 8080, 8081, 4040 to the host's ports 8888, 8080, 8081, 4040 respectively.

### Start coding

Open your browser and go to http://localhost:8888/ to start coding with Sedona.

### Notes

* This container assumes you have at least 8GB RAM and takes all your CPU cores and 8GM RAM.
* Sedona in this container runs in the cluster mode. Only 1 notebook can be run at a time. If you want to run another notebook, please shut down the kernel of the current notebook first ([How?](https://jupyterlab.readthedocs.io/en/stable/user/running.html)).

## How to build

Clone the Sedona GitHub repository

### Build the image against a Sedona release

Requirements: docker ([How?](https://docs.docker.com/engine/install/))

Format: 

```bash
./docker/sedona-spark-jupyterlab/build.sh <spark_version> <sedona_version>
```

Example:

```bash
./docker/sedona-spark-jupyterlab/build.sh 3.4.1 1.4.1
```

### Build the image against the latest Sedona master

Requirements: docker ([How?](https://docs.docker.com/engine/install/)), JDK <= 19, maven3

Format:

```bash
./docker/sedona-spark-jupyterlab/build.sh <spark_version> latest
```

Example: 

```bash
./docker/sedona-spark-jupyterlab/build.sh 3.4.1 latest
```

### Notes

This docker image can only be built against Sedona 1.4.1+ and Spark 3.0+

## Cluster Configuration

### Software
* OS: Ubuntu 22.02
* JDK: openjdk-19
* Python: 3.10

### Web UI
* JupyterLab: http://localhost:8888/
* Spark master URL: spark://localhost:7077
* Spark job UI: http://localhost:4040
* Spark master web UI: http://localhost:8080/
* Spark web UI: http://localhost:8081/