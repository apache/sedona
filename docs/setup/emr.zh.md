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

在 EMR 上推荐使用 Sedona-1.3.1-incubating 及以上版本。本教程使用 AWS Elastic MapReduce (EMR) 6.9.0，已预装以下应用：Hadoop 3.3.3、JupyterEnterpriseGateway 2.6.0、Livy 0.7.1、Spark 3.3.0。

本教程在带 EMR Studio（notebook）的 EMR on EC2 上完成测试。EMR on EC2 使用 YARN 进行资源管理。

!!!note
	如果您使用 Spark 3.4+ 与 Scala 2.12，请使用 `sedona-spark-shaded-3.4_2.12`。请注意 Spark 与 Scala 的版本后缀。

## 准备初始化脚本

在您的 S3 存储桶中添加一个内容如下的脚本：

```bash
#!/bin/bash

# EMR 集群只有临时的本地存储，jar 放在哪个路径其实并不影响。
sudo mkdir /jars

# 下载 Sedona jar
sudo curl -o /jars/sedona-spark-shaded-3.3_2.12-{{ sedona.current_version }}.jar "https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.3_2.12/{{ sedona.current_version }}/sedona-spark-shaded-3.3_2.12-{{ sedona.current_version }}.jar"

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
      "spark.yarn.dist.jars": "/jars/sedona-spark-shaded-3.3_2.12-{{ sedona.current_version }}.jar,/jars/geotools-wrapper-{{ sedona.current_geotools }}.jar",
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
