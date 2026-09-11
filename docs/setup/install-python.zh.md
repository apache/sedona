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

基础包 `apache-sedona` 会安装 `shapely` 和 `attrs`。在 Spark 上使用 Sedona 还需要 PySpark，托管 Spark 平台通常已预装该依赖。

使用 `pip` 安装已发布的包及所需的附加依赖。Sedona 项目开发使用 [uv](https://docs.astral.sh/uv/) 管理依赖。包依赖及版本约束见 [python/pyproject.toml](https://github.com/apache/sedona/blob/master/python/pyproject.toml)。

### 安装 sedona

* 从 PyPI 仓库安装。最新版的 Sedona Python 可在 [PyPI](https://pypi.org/project/apache-sedona/) 找到。[Sedona v1.0.1 及更早版本存在已知问题](release-notes.md#known-issue)。

```bash
pip install apache-sedona
```

* 自 Sedona v1.1.0 起，pyspark 已成为 Sedona Python 的可选依赖，因为许多 Spark 平台都已预装 Spark。如需在安装 Sedona Python 时一并安装 pyspark，请使用 `spark` 附加项：

```bash
pip install "apache-sedona[spark]"
```

* 从 Sedona Python 源码安装

克隆 Sedona GitHub 源码后运行以下命令：

```bash
cd python
python3 -m pip install .
```

### 可选依赖 { #optional-dependencies }

附加依赖（extra）会为特定用途安装额外的包，请按需选择：

| 包或附加依赖 | 安装的额外包 | 用途 |
| :--- | :--- | :--- |
| `apache-sedona` | `shapely`、`attrs` | 基础包，可配合现有 Spark 环境使用。 |
| `spark` | `pyspark` | 在尚未安装 PySpark 时安装。 |
| `pydeck-map` | `geopandas`、`pydeck` | 使用 `SedonaPyDeck` 创建地图。 |
| `kepler-map` | `geopandas`、`keplergl` | 使用 `SedonaKepler` 创建地图。 |
| `flink` | `apache-flink` | 在 PyFlink 上使用 Sedona。 |
| `db` | `sedonadb[geopandas]` | 安装支持 GeoPandas 的 SedonaDB；仅在 Python 3.9 及以上版本请求安装。 |
| `all` | `pyspark`、`geopandas`、`pydeck`、`keplergl`、`rasterio` | 一并安装 Spark、地图和 Python 栅格相关依赖。 |

例如，安装 `all` 附加依赖：

```bash
pip install "apache-sedona[all]"
```

虽然名为 `all`，它并不包含 `flink` 或 `db` 附加依赖。附加依赖也不会安装下文所述的 Sedona JVM jar 文件。

`rasterio` 用于支持 Python 端的栅格对象。使用 SQL 读取现有栅格文件不需要它。

在 EMR 等托管 Spark 平台上，请保留平台提供的 PySpark。若要安装两个地图包而不请求安装 PySpark，可以组合使用地图附加依赖：

```bash
pip install "apache-sedona[pydeck-map,kepler-map]"
```

GeoPandas 会通过依赖关系安装 `pandas`。基于 Arrow 的转换和 pandas-on-Spark API 还需要 PyArrow；`spark` 和 `all` 附加依赖不会显式请求安装它。使用这些 API 时，请安装与 Spark 版本兼容的 `pandas` 和 `pyarrow`。转换示例见[使用 GeoPandas 和 Shapely](../tutorial/geopandas-shapely.md)。

### 准备 sedona-spark jar

Sedona Python 需要额外配套一个名为 `sedona-spark-shaded` 或 `sedona-spark` 的 jar 文件。请确保所选版本与您的 Spark 和 Scala 版本匹配。

请在 artifact 名称中使用 Spark 的 major.minor 版本号。

可通过以下方式之一获取该 jar：

1. 如果您在 Databricks、AWS EMR 或其他云平台的 notebook 中运行 Sedona，请使用 `shaded jar`：从 Maven Central 下载 [sedona-spark-shaded jar](https://repo.maven.apache.org/maven2/org/apache/sedona/) 与 [geotools-wrapper jar](https://repo.maven.apache.org/maven2/org/datasyslab/geotools-wrapper/)，并将其放入 `SPARK_HOME/jars/` 目录。
2. 如果您在 IDE 或本地 Jupyter Notebook 中运行 Sedona，请使用 `unshaded jar`，并在 Python 程序中通过 [Maven Central 坐标](maven-coordinates.md) 引用，例如：

==Sedona >= 1.4.1==

```python
from sedona.spark import *

config = (
    SedonaContext.builder()
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-3.5_2.12:{{ sedona.current_version }},"
        "org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}",
    )
    .config(
        "spark.jars.repositories",
        "https://artifacts.unidata.ucar.edu/repository/unidata-all",
    )
    .getOrCreate()
)
sedona = SedonaContext.create(config)
```

==Sedona < 1.4.1==

`SedonaRegistrator` 自 Sedona 1.4.1 起已被弃用，请改用上面的方式。

```python
from pyspark.sql import SparkSession
from sedona.spark import SedonaRegistrator
from sedona.spark import SedonaKryoRegistrator, KryoSerializer

spark = (
    SparkSession.builder.appName("appName")
    .config("spark.serializer", KryoSerializer.getName)
    .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-shaded-3.5_2.12:{{ sedona.current_version }},"
        "org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}",
    )
    .getOrCreate()
)
SedonaRegistrator.registerAll(spark)
```

### 设置环境变量

如果您手动将 sedona-spark-shaded jar 复制到 `SPARK_HOME/jars/` 目录，则需要设置两个环境变量：

* SPARK_HOME。例如，在终端运行：

```bash
export SPARK_HOME=~/Downloads/spark-3.0.1-bin-hadoop2.7
```

* PYTHONPATH。例如，在终端运行：

```bash
export PYTHONPATH=$SPARK_HOME/python
```

之后即可体验 [Sedona Python Jupyter Notebook](../tutorial/jupyter-notebook.md)。
