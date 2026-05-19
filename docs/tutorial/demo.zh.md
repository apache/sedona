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

# Scala 与 Java 示例

[Scala 与 Java 示例](https://github.com/apache/sedona/tree/master/examples) 中包含了 Sedona Spark（RDD、SQL 与 Viz）以及 Sedona Flink 的模板项目，且这些模板项目已经做好了适当的配置。

注意：尽管这些模板项目使用 Scala 编写，相同的 API 在 Java 中同样适用。

## 目录结构

仓库的目录结构如下：

* spark-sql：Scala 模板，演示如何使用 Sedona 的 RDD、DataFrame 与 SQL API
* flink-sql：Java 模板，演示如何通过 Flink Table API 使用 Sedona SQL

## 编译与打包

### 前置条件

请确保您的本机已安装以下软件：

* Scala 项目：Scala 2.12
* Java 项目：JDK 1.8、Apache Maven 3

### 编译

在每个模板项目目录下运行 `mvn clean package`。

### 提交 fat jar 到 Spark

执行上述命令后，可在 `./target` 目录中看到生成的 fat jar，使用 `./bin/spark-submit` 提交即可。

如此提交需要做以下调整：

* 修改模板项目中的 Spark Master 地址，或者直接删除该配置。当前模板硬编码为 `local[*]`，表示在本地以全部核心运行。
* 将 Apache Spark 依赖的打包范围从 `compile` 改为 `provided`。这是 Maven 与 SBT 中常见的打包策略，意为不要把 Spark 一并打入 fat jar，否则可能导致 jar 体积过大并引发版本冲突。
* 确保 build.sbt 中的依赖版本与您的 Spark 版本一致。

## 在本地运行模板项目

强烈建议使用 IDE 在本机运行模板项目。Scala 推荐使用安装了 Scala 插件的 IntelliJ IDEA；Java 推荐使用 IntelliJ IDEA 或 Eclipse。借助 IDE，**您无需做任何额外准备**（甚至不需要下载和搭建 Spark）！只要安装好 Scala 与 Java，一切都能正常运行。

### Scala

将 Scala 模板项目以 SBT 项目形式导入，然后运行其中的 Main 文件。
