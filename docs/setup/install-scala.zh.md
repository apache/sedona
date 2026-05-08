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

在开始 Sedona 之旅之前，请确保您的 Apache Spark 集群已就绪。

在 Apache Spark 中使用 Scala/Java 库有两种方式，您可以任选其一来运行 Sedona：

* Spark 交互式 Scala 或 SQL shell：上手简单，适合新手尝试简单的函数调用。
* 独立的 Scala / Java 项目：包管理学习成本较高，但适合大型项目。

## Spark Scala shell

### 自动下载 Sedona jar

1. 准备好 Spark 集群。

2. 使用 `--packages` 选项启动 Spark shell。该命令会自动从 Maven Central 下载 Sedona jar。

```
./bin/spark-shell --packages MavenCoordinates
```

请参考 [Sedona Maven Central 坐标](maven-coordinates.md)，根据您的 Spark 版本选择对应的 Sedona 包。

    * 本地模式：无需搭建集群即可测试 Sedona
    ```
    ./bin/spark-shell --packages org.apache.sedona:sedona-spark-shaded-3.3_2.12:{{ sedona.current_version }},org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}
    ```

    * 集群模式：需要指定 Spark Master IP
    ```
    ./bin/spark-shell --master spark://localhost:7077 --packages org.apache.sedona:sedona-spark-shaded-3.3_2.12:{{ sedona.current_version }},org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}
    ```

### 手动下载 Sedona jar

1. 准备好 Spark 集群。

2. 下载 Sedona jar：
	* 从 [Sedona Releases](../download.md) 下载预编译 jar
	* 下载 / git clone Sedona 源码并自行编译（参见 [编译 Sedona](compile.md)）
3. 使用 `--jars` 选项启动 Spark shell。

```
./bin/spark-shell --jars /Path/To/SedonaJars.jar
```

请使用文件名中包含 Spark major.minor 版本号的 jar，例如 `sedona-spark-shaded-3.3_2.12-{{ sedona.current_version }}`。

    * 本地模式：无需搭建集群即可测试 Sedona
    ```
    ./bin/spark-shell --jars /path/to/sedona-spark-shaded-3.3_2.12-{{ sedona.current_version }}.jar,/path/to/geotools-wrapper-{{ sedona.current_geotools }}.jar
    ```

    * 集群模式：需要指定 Spark Master IP
    ```
    ./bin/spark-shell --master spark://localhost:7077 --jars /path/to/sedona-spark-shaded-3.3_2.12-{{ sedona.current_version }}.jar,/path/to/geotools-wrapper-{{ sedona.current_geotools }}.jar
    ```

## Spark SQL shell

请参阅 [在纯 SQL 环境中使用 Sedona](../tutorial/sql-pure-sql.md)。

## 独立的 Spark 项目

独立项目可以让您创建多个 Scala / Java 文件，并将复杂的逻辑集中实现。要在独立的 Spark 项目中使用 Sedona，只需在 `pom.xml` 或 `build.sbt` 中将 Sedona 添加为依赖。

1. 关于如何添加 Sedona 依赖，请参阅 [Sedona Maven Central 坐标](maven-coordinates.md)
2. 使用 Sedona 模板项目快速上手：[Sedona 模板项目](../tutorial/demo.md)
3. 使用 SBT 编译您的项目，并确保得到打包了所有依赖的 fat jar。
4. 将编译生成的 fat jar 提交到 Spark 集群。请确保位于 Spark 发行版的根目录，然后运行以下命令：

```
./bin/spark-submit --master spark://YOUR-IP:7077 /Path/To/YourJar.jar
```

!!!note
	关于 spark-submit 的详细说明请参见 [Spark 官方文档](https://spark.apache.org/docs/latest/submitting-applications.html)。
