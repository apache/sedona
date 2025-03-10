<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Sedona JupyterLab Docker Image

Sedona Docker images are available on [Sedona official DockerHub repo](https://hub.docker.com/r/apache/sedona).

We provide a Docker image for Apache Sedona with Python JupyterLab, Apache Zeppelin and 1 master node and 1 worker node.

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
docker run -d -e DRIVER_MEM=<driver_mem> -e EXECUTOR_MEM=<executor_mem> -p 8888:8888 -p 8080:8080 -p 8081:8081 -p 4040:4040 -p 8085:8085 apache/sedona:<sedona_version>
```

Driver memory and executor memory are optional. If their values are not given, the container will take 4GB RAM for the driver and 4GB RAM for the executor. The -d (or --detach) flag ensures the container runs in detached mode, allowing it to run in the background.

Example 1:

```bash
docker run -d -e DRIVER_MEM=6g -e EXECUTOR_MEM=8g -p 8888:8888 -p 8080:8080 -p 8081:8081 -p 4040:4040 -p 8085:8085 apache/sedona:latest
```

This command will start a container with 6GB RAM for the driver and 8GB RAM for the executor and use the latest Sedona image. The container will run in detached mode.

This command will bind the container's ports 8888, 8080, 8081, 4040, 8085 to the host's ports 8888, 8080, 8081, 4040, 8085 respectively.

Example 2:

```bash
docker run -d -e -p 8888:8888 -p 8080:8080 -p 8081:8081 -p 4040:4040 -p 8085:8085 apache/sedona:{{ sedona.current_version }}
```

This command will start a container with 4GB RAM for the driver and 4GB RAM for the executor and use Sedona {{ sedona.current_version }} image.

This command will bind the container's ports 8888, 8080, 8081, 4040, 8085 to the host's ports 8888, 8080, 8081, 4040, 8085 respectively.

Example 3: Persisting `/opt` (Jupyter & Zeppelin Data) with Docker Volume

To ensure that **Jupyter workspace, Zeppelin notebooks, and configurations persist**, mount `/opt` as a **Docker volume**:

```bash
docker run -d -e DRIVER_MEM=6g -e EXECUTOR_MEM=8g \
    -p 8888:8888 -p 8080:8080 -p 8081:8081 -p 4040:4040 -p 8085:8085 \
    -v sedona_opt:/opt \
    apache/sedona:latest
```

- The `-v sedona_opt:/opt` flag **creates (if not existing) and mounts a Docker volume named `sedona_opt`** to the `/opt` directory inside the container.
- This ensures that **Jupyter and Zeppelin notebooks, configurations, and workspaces persist** even if the container is stopped or removed.

### Start coding

Open your browser and go to [http://localhost:8888/](http://localhost:8888/) to start coding with Sedona in Jupyter Notebook. You can also access Apache Zeppelin at [http://localhost:8085/classic/](http://localhost:8085/classic/  ) using your browser.

### Notes

- This container assumes you have at least 8GB RAM and takes all your CPU cores and 8GM RAM. The 1 worker will take 4GB and the Jupyter program will take the remaining 4GB.
- Sedona in this container runs in the cluster mode. Only 1 notebook can be run at a time. If you want to run another notebook, please shut down the kernel of the current notebook first ([How?](https://jupyterlab.readthedocs.io/en/stable/user/running.html)).

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

This docker image can only be built against Sedona 1.7.0+ and Spark 3.3+

## Cluster Configuration

### Software

- OS: Ubuntu 22.02
- JDK: openjdk-19
- Python: 3.10
- Spark 3.4.1

### Web UI

- JupyterLab: http://localhost:8888/
- Spark master URL: spark://localhost:7077
- Spark job UI: http://localhost:4040
- Spark master web UI: http://localhost:8080/
- Spark work web UI: http://localhost:8081/
- Apache Zeppelin: http://localhost:8085/

A Zeppelin tutorial notebook is bundled with Sedona tutorials. See [Sedona-Zeppelin tutorial](../tutorial/zeppelin.md) for details.

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
