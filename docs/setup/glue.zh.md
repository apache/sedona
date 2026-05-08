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

本教程介绍如何配置 Glue notebook 与 Glue ETL 作业。本文假设您已具备 AWS Glue 作业的基础使用经验。

教程中使用 Sedona {{ sedona.current_version }} 与 [Glue 5.0](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html)，运行环境为 Spark 3.5.4、Java 17、Scala 2.12 和 Python 3.11。Glue 5.0 推荐使用 Sedona 1.8.0 及以上版本。

!!!warning
    **重要：** 自 Sedona 1.8.0 起不再支持 Java 8 与 Spark 3.3。Sedona 1.8.0+ 需要 Glue 5.0+（支持 Java 17 与 Spark 3.5+）。如果您必须停留在 Glue 4.0（Spark 3.3、Java 8），请使用 Sedona 1.7.1 或更低版本，并将下方的 `sedona-spark-shaded-3.5_2.12` artifact 替换为 `sedona-spark-shaded-3.3_2.12`。

## 收集 Maven 链接

您需要让 Glue 作业指向 Sedona 与 Geotools 的 jar。推荐直接使用 Maven 上发布的 jar。下面给出的链接是为 Glue 5.0 准备的。

Sedona Jar：[Maven Central](https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/{{ sedona.current_version }}/sedona-spark-shaded-3.5_2.12-{{ sedona.current_version }}.jar)

Geotools Jar：[Maven Central](https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/{{ sedona.current_geotools }}/geotools-wrapper-{{ sedona.current_geotools }}.jar)

!!!note
    请务必选择 Scala 2.12 与 Spark 3.5 的版本。Scala 2.13 的 jar 与 Glue 5.0 不兼容。

## 配置 Glue 作业

获得 jar 链接后，您可以将它们与 apache-sedona Python 包一起配置到 Glue 作业中。notebook 类型与 script 类型的作业配置方式略有不同。

!!!note
    请务必保持 jar 与 Python 包的 Sedona 版本一致。

### Notebook 作业

在创建 sparkContext 或 glueContext 之前，添加以下 cell magic。第一行指向 jar，第二行直接通过 pip 安装 Sedona Python 包。

```text
# Sedona Config
%extra_jars https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/{{ sedona.current_version }}/sedona-spark-shaded-3.5_2.12-{{ sedona.current_version }}.jar, https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/{{ sedona.current_geotools }}/geotools-wrapper-{{ sedona.current_geotools }}.jar
%additional_python_modules apache-sedona=={{ sedona.current_version }}
```

如果您使用 Glue 提供的示例 notebook，则第一个 cell 现在大致应是：

```text
%idle_timeout 2880
%glue_version 5.0
%worker_type G.1X
%number_of_workers 5

# Sedona Config
%extra_jars https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/{{ sedona.current_version }}/sedona-spark-shaded-3.5_2.12-{{ sedona.current_version }}.jar, https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/{{ sedona.current_geotools }}/geotools-wrapper-{{ sedona.current_geotools }}.jar
%additional_python_modules apache-sedona=={{ sedona.current_version }}


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
```

可以通过运行以下 cell 验证安装：

```python
from sedona.spark import *

sedona = SedonaContext.create(spark)
sedona.sql("SELECT ST_POINT(1., 2.) as geom").show()
```

### ETL 作业

Glue 也将这种作业称为 Script 作业。在作业页面上切换到 “Job details” 选项卡，展开页面底部的 “Advanced properties” 部分。在 “Dependent JARs path” 字段中填入两个 jar 的链接，并以英文逗号分隔。

要添加 Sedona Python 包，进入 “Job Parameters” 部分新增一个参数，键为 `--additional-python-modules`，值为 `apache-sedona=={{ sedona.current_version }}`。

为验证安装，可在脚本中加入以下代码：

```python
from sedona.spark import *

config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)

sedona.sql("SELECT ST_POINT(1., 2.) as geom").show()
```

将其加入脚本后保存并运行作业。如果作业运行成功，则说明安装成功。
