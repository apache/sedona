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

本教程使用 AWS Elastic MapReduce (EMR) 7.9.0，已预装以下应用：Hadoop 3.4.1、JupyterEnterpriseGateway 2.6.0、Livy 0.8.0-incubating、Spark 3.5.5。任何 EMR 7.x 版本均可，因为它们都搭载 Spark 3.5。

本教程在带 EMR Studio（notebook）的 EMR on EC2 上完成测试。EMR on EC2 使用 YARN 进行资源管理。

!!!note
	请使用 Spark 主.次版本与集群一致的 artifact：Spark 3.5 用 `sedona-spark-shaded-3.5_2.12`，Spark 4.0 用 `sedona-spark-shaded-4.0_2.13`，Spark 4.1 用 `sedona-spark-shaded-4.1_2.13`。Spark 4.0 及以上仅提供 Scala 2.13 构建。

## 准备初始化脚本

在您的 S3 存储桶中添加一个内容如下的脚本：

```bash
#!/bin/bash

# EMR 集群只有临时的本地存储，jar 放在哪个路径其实并不影响。
sudo mkdir /jars

# 下载 Sedona jar
sudo curl -o /jars/sedona-spark-shaded-3.5_2.12-{{ sedona.current_version }}.jar "https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/{{ sedona.current_version }}/sedona-spark-shaded-3.5_2.12-{{ sedona.current_version }}.jar"

# 下载 GeoTools jar
sudo curl -o /jars/geotools-wrapper-{{ sedona.current_geotools }}.jar "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/{{ sedona.current_geotools }}/geotools-wrapper-{{ sedona.current_geotools }}.jar"

# 安装必要的 Python 库
sudo python3 -m pip install pandas
sudo python3 -m pip install shapely
sudo python3 -m pip install geopandas
sudo python3 -m pip install keplergl==0.3.2
sudo python3 -m pip install pydeck==0.8.0
sudo python3 -m pip install attrs matplotlib descartes apache-sedona=={{ sedona.current_version }}
```

创建 EMR 集群时，在 `bootstrap action` 中指定该脚本的位置。

## 添加软件配置

创建 EMR 集群时，在软件配置（software configuration）中加入以下内容：

```bash
[
  {
    "Classification":"spark-defaults",
    "Properties":{
      "spark.yarn.dist.jars": "/jars/sedona-spark-shaded-3.5_2.12-{{ sedona.current_version }}.jar,/jars/geotools-wrapper-{{ sedona.current_geotools }}.jar",
      "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
      "spark.kryo.registrator": "org.apache.sedona.core.serde.SedonaKryoRegistrator",
      "spark.sql.extensions": "org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions"
      }
  }
]
```

## 验证安装

集群创建完成后，可在 Jupyter Notebook 中运行以下代码以验证安装：

```python
spark.sql("SELECT ST_Point(0, 0)").show()
```

注意：您不需要再调用 `SedonaRegistrator.registerAll(spark)` 或 `SedonaContext.create(spark)`，因为配置中的 `org.apache.sedona.sql.SedonaSqlExtensions` 已经为您完成了这些工作。

## 在 R 中使用 Sedona

[`apache.sedona`](https://cran.r-project.org/package=apache.sedona) R 包是一个 [`sparklyr`](https://spark.rstudio.com) 扩展。只要在 `spark_connect()` 之前加载它，Sedona 的序列化器、UDT 与 UDF 就会自动注册，因此 R 中不需要手动调用与 `SedonaContext.create()` 等价的方法。

!!!note
	R 接口仅支持 Spark 3.x。请确认您选择的 EMR 版本自带的是 Spark 3 系列版本。

### 扩展初始化脚本

在上面的引导脚本中追加以下内容，以便在运行 R 的节点上安装 R 及这两个 R 包：

```bash
# 安装 R 以及 Sedona 的 R 接口
sudo yum install -y R
sudo R -e 'install.packages(c("sparklyr", "apache.sedona"), repos = "https://cloud.r-project.org")'
```

### 从 R 连接集群

EMR 将 Spark 安装在 `/usr/lib/spark` 下。请将 `SEDONA_JAR_FILES` 指向引导脚本已经下载到 `/jars` 的 jar 包，这样 `sparklyr` 就会直接使用它们，而不必在每次连接时从 Maven Central 解析 Sedona 坐标：

```r
library(sparklyr)
library(apache.sedona)

Sys.setenv(
  "SEDONA_JAR_FILES" = paste(
    "/jars/sedona-spark-shaded-3.3_2.12-{{ sedona.current_version }}.jar",
    "/jars/geotools-wrapper-{{ sedona.current_geotools }}.jar",
    sep = ":"
  )
)

sc <- spark_connect(master = "yarn", spark_home = "/usr/lib/spark")
```

!!!note
	`SEDONA_JAR_FILES` 是一个以 `:` 分隔的列表，并且会同时替换 `apache.sedona` 原本请求的*两个* Maven 坐标，因此必须把 GeoTools wrapper 的 jar 与 Sedona 的 jar 一起列出。如果不设置 `SEDONA_JAR_FILES`，每次连接都会下载 `org.apache.sedona:sedona-spark-shaded-<spark 版本>_<scala 版本>:{{ sedona.current_version }}` 与 `org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}`。这要求 driver 能够访问外网，并且耗时可能超过 `sparklyr.connect.timeout` 的默认值。

### 验证 R 端安装

```r
sdf_sql(sc, "SELECT ST_Point(0.0, 0.0) AS geom") %>% collect()
```

关于 R 接口的更多功能，请参阅 [Sedona R 文档](https://sedona.apache.org/latest/api/rdocs/)。
