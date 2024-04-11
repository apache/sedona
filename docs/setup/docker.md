# Sedona JupyterLab Docker Image

Sedona Docker images are available on [Sedona official DockerHub repo](https://hub.docker.com/r/apache/sedona).

We provide a Docker image for Apache Sedona with Python JupyterLab and 1 master node and 1 worker node.

## How to use

### Pull the image from DockerHub

Format:

```bash
docker pull apache/sedona:<sedona_version>
```

Example 1: Pull the latest image of Sedona master branch

```bash
docker pull apache/sedona:latest
```

Example 2: Pull the image of a specific Sedona release

```bash
docker pull apache/sedona:{{ sedona.current_version }}
```

### Start the container

Format:

```bash
docker run -e DRIVER_MEM=<driver_mem> -e EXECUTOR_MEM=<executor_mem> -p 8888:8888 -p 8080:8080 -p 8081:8081 -p 4040:4040 apache/sedona:<sedona_version>
```

Driver memory and executor memory are optional. If their values are not given, the container will take 4GB RAM for the driver and 4GB RAM for the executor.

Example 1:

```bash
docker run -e DRIVER_MEM=6g -e EXECUTOR_MEM=8g -p 8888:8888 -p 8080:8080 -p 8081:8081 -p 4040:4040 apache/sedona:latest
```

This command will start a container with 6GB RAM for the driver and 8GB RAM for the executor and use the latest Sedona image.

This command will bind the container's ports 8888, 8080, 8081, 4040 to the host's ports 8888, 8080, 8081, 4040 respectively.

Example 2:

```bash
docker run -p 8888:8888 -p 8080:8080 -p 8081:8081 -p 4040:4040 apache/sedona:{{ sedona.current_version }}
```

This command will start a container with 4GB RAM for the driver and 4GB RAM for the executor and use Sedona {{ sedona.current_version }} image.

This command will bind the container's ports 8888, 8080, 8081, 4040 to the host's ports 8888, 8080, 8081, 4040 respectively.

### Start coding

Open your browser and go to [http://localhost:8888/](http://localhost:8888/) to start coding with Sedona.

### Notes

* This container assumes you have at least 8GB RAM and takes all your CPU cores and 8GM RAM. The 1 worker will take 4GB and the Jupyter program will take the remaining 4GB.
* Sedona in this container runs in the cluster mode. Only 1 notebook can be run at a time. If you want to run another notebook, please shut down the kernel of the current notebook first ([How?](https://jupyterlab.readthedocs.io/en/stable/user/running.html)).

## How to build

Clone the Sedona GitHub repository

### Build the image against a Sedona release

Requirements: docker ([How?](https://docs.docker.com/engine/install/))

Format:

```bash
./docker/sedona-spark-jupyterlab/build.sh <spark_version> <sedona_version> <build_mode>
```

Example:

```bash
./docker/sedona-spark-jupyterlab/build.sh 3.4.1 {{ sedona.current_version }}
```

`build_mode` is optional. If its value is not given or is `local`, the script will build the image locally. Otherwise, it will start a cross-platform compilation and push images directly to DockerHub.

### Build the image against the latest Sedona master

Requirements: docker ([How?](https://docs.docker.com/engine/install/)), JDK <= 19, maven3

Format:

```bash
./docker/sedona-spark-jupyterlab/build.sh <spark_version> latest <build_mode>
```

Example:

```bash
./docker/sedona-spark-jupyterlab/build.sh 3.4.1 latest
```

`build_mode` is optional. If its value is not given or is `local`, the script will build the image locally. Otherwise, it will start a cross-platform compilation and push images directly to DockerHub.

### Notes

This docker image can only be built against Sedona 1.4.1+ and Spark 3.0+

## Cluster Configuration

### Software

* OS: Ubuntu 22.02
* JDK: openjdk-19
* Python: 3.10
* Spark 3.4.1

### Web UI

* JupyterLab: http://localhost:8888/
* Spark master URL: spark://localhost:7077
* Spark job UI: http://localhost:4040
* Spark master web UI: http://localhost:8080/
* Spark work web UI: http://localhost:8081/

## How to push to DockerHub

Format:

```bash
docker login
./docker/sedona-spark-jupyterlab/build.sh <spark_version> <sedona_version> release
```

Example:

```bash
docker login
./docker/sedona-spark-jupyterlab/build.sh 3.4.1 {{ sedona.current_version }} release
```
