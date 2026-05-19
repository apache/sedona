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

# Sedona JupyterLab Docker 镜像

Sedona 的 Docker 镜像可在 [Sedona 官方 DockerHub 仓库](https://hub.docker.com/r/apache/sedona) 获取。

我们提供了一个集成 Python JupyterLab、Apache Zeppelin，以及 1 个 master 节点和 1 个 worker 节点的 Apache Sedona Docker 镜像。

## 使用方法

### 从 DockerHub 拉取镜像

格式：

```bash
docker pull apache/sedona:<sedona_version>
```

示例 1：拉取 Sedona master 分支的最新镜像

```bash
docker pull apache/sedona:latest
```

示例 2：拉取某个特定 Sedona 发布版本的镜像

```bash
docker pull apache/sedona:{{ sedona.current_version }}
```

### 启动容器

格式：

```bash
docker run -d -e DRIVER_MEM=<driver_mem> -e EXECUTOR_MEM=<executor_mem> -p 8888:8888 -p 8080:8080 -p 8081:8081 -p 4040:4040 -p 8085:8085 apache/sedona:<sedona_version>
```

`driver_mem` 与 `executor_mem` 是可选项；如果未提供，容器将分别为 driver 和 executor 各分配 4GB 内存。`-d`（或 `--detach`）参数让容器以分离模式后台运行。

示例 1：

```bash
docker run -d -e DRIVER_MEM=6g -e EXECUTOR_MEM=8g -p 8888:8888 -p 8080:8080 -p 8081:8081 -p 4040:4040 -p 8085:8085 apache/sedona:latest
```

该命令会启动一个使用 6GB driver 内存与 8GB executor 内存的容器，并使用最新的 Sedona 镜像；容器以分离模式运行。

该命令会将容器的 8888、8080、8081、4040、8085 端口分别映射到宿主机的同名端口。

示例 2：

```bash
docker run -d -e -p 8888:8888 -p 8080:8080 -p 8081:8081 -p 4040:4040 -p 8085:8085 apache/sedona:{{ sedona.current_version }}
```

该命令会启动一个 driver 与 executor 各使用 4GB 内存的容器，并使用 Sedona {{ sedona.current_version }} 镜像。

容器的端口映射与示例 1 相同。

示例 3：使用 Docker volume 持久化 `/opt`（Jupyter & Zeppelin 数据）

为确保 **Jupyter 工作区、Zeppelin notebook 与配置不会丢失**，可以将 `/opt` 挂载为 **Docker volume**：

```bash
docker run -d -e DRIVER_MEM=6g -e EXECUTOR_MEM=8g \
    -p 8888:8888 -p 8080:8080 -p 8081:8081 -p 4040:4040 -p 8085:8085 \
    -v sedona_opt:/opt \
    apache/sedona:latest
```

- `-v sedona_opt:/opt` 会**创建（若不存在）并挂载名为 `sedona_opt` 的 Docker volume** 到容器内的 `/opt` 目录。
- 即使容器被停止或删除，**Jupyter 与 Zeppelin 的 notebook、配置和工作区也会持久保留**。

### 开始编码

打开浏览器访问 [http://localhost:8888/](http://localhost:8888/)，即可在 Jupyter Notebook 中使用 Sedona 编码。也可以在浏览器中访问 [http://localhost:8085/classic/](http://localhost:8085/classic/) 来使用 Apache Zeppelin。

### 注意事项

- 该容器假设您的机器至少有 8GB 内存，并会占用所有 CPU 核心与 8GB 内存。1 个 worker 占用 4GB，Jupyter 程序占用剩余的 4GB。
- 容器中的 Sedona 以集群模式运行，同一时间只能运行 1 个 notebook。如需运行其他 notebook，请先关闭当前 notebook 的内核（[操作方法](https://jupyterlab.readthedocs.io/en/stable/user/running.html)）。

## 构建方法

克隆 Sedona GitHub 仓库。

### 针对某个 Sedona 发布版构建镜像

要求：docker（[安装方法](https://docs.docker.com/engine/install/)）

格式：

```bash
./docker/build.sh <spark_version> <sedona_version> <build_mode>
```

示例：

```bash
./docker/build.sh 3.4.1 {{ sedona.current_version }}
```

`build_mode` 为可选项。如果未提供或值为 `local`，脚本会在本地构建镜像；否则会启动跨平台编译并直接将镜像推送到 DockerHub。

### 针对最新的 Sedona master 构建镜像

要求：docker（[安装方法](https://docs.docker.com/engine/install/)）、JDK <= 19、maven3

格式：

```bash
./docker/build.sh <spark_version> latest <build_mode>
```

示例：

```bash
./docker/build.sh 3.4.1 latest
```

`build_mode` 为可选项，含义与上文相同。

### 注意事项

该 Docker 镜像仅支持基于 Sedona 1.7.0+ 与 Spark 3.3+ 构建。

## 集群配置

### 软件

- OS：Ubuntu 22.02
- JDK：openjdk-19
- Python：3.10
- Spark 3.5.5

### Web UI

- JupyterLab：http://localhost:8888/
- Spark master URL：spark://localhost:7077
- Spark 作业 UI：http://localhost:4040
- Spark master Web UI：http://localhost:8080/
- Spark worker Web UI：http://localhost:8081/
- Apache Zeppelin：http://localhost:8085/

容器中已捆绑一份 Zeppelin 教程 notebook，详情请参阅 [Sedona-Zeppelin 教程](../tutorial/zeppelin.md)。

## 如何推送到 DockerHub

格式：

```bash
docker login
./docker/build.sh <spark_version> <sedona_version> release
```

示例：

```bash
docker login
./docker/build.sh 3.4.1 {{ sedona.current_version }} release
```
