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

# SedonaFlink

SedonaFlink 将地理空间函数集成到 Apache Flink 中，是构建依赖地理空间数据的流式管道的理想选择。

以下是 SedonaFlink 的一些典型应用场景：

* 从 Kafka 读取地理空间数据并写入 Iceberg
* [实时分析交通密度](https://www.alibabacloud.com/help/en/flink/realtime-flink/use-cases/analyze-traffic-density-with-flink-and-apache-sedona)
* 电信行业的实时网络规划与优化

下面是一些示例代码片段：

=== "Java"

    ```java
    sedona.createTemporaryView("myTable", tbl)
    Table geomTbl = sedona.sqlQuery("SELECT ST_GeomFromWKT(geom_polygon) as geom_polygon, name_polygon FROM myTable")
    geomTbl.execute().print()
    ```

=== "PyFlink"

    ```python
    table_env.sql_query("SELECT ST_ASBinary(ST_Point(1.0, 2.0))").execute().collect()
    ```

## 主要特性

* **实时地理空间流处理**，满足低延迟处理需求。
* **可扩展**，适用于大规模流式管道。
* 基于 Flink 的时间窗口实现**事件时间处理**。
* 提供**精确一次（Exactly Once）**处理保证。
* **可移植**，易于在任何 Flink 运行时中部署。
* **开源**，遵循 Apache 软件基金会的准则进行管理。

## 为什么在 Flink 上使用 Sedona？

Flink 是为流式数据而设计的，Sedona 在此基础上为其加入了地理空间处理能力。

大多数地理空间处理发生在 Spark 或 PostGIS 这类批处理系统中，对于延迟要求不高的场景这没有问题。

而当需要实时处理地理空间数据时，Sedona on Flink 的优势就会显现出来。

Flink 可以为地理空间查询提供毫秒级的延迟。

Flink 拥有完善的容错机制，即使在故障发生时，地理空间管道也不会丢失数据。

Sedona on Flink 可以在 Flink 支持的任何环境中运行，包括 Kubernetes、YARN 以及独立集群。

## 工作原理

Sedona 直接集成到 Flink 的 Table API 和 SQL 引擎中。

在搭建 Flink 环境时注册 Sedona 的空间函数，即可在 SQL 查询中使用 `ST_Point`、`ST_Contains`、`ST_Distance` 等函数。

Sedona 同时支持 Flink 的 DataStream API 与 Table API，可根据自身工作流程选用。

空间运算作为 Flink 分布式执行的一部分运行，因此地理空间计算会自动在集群中并行执行。

Sedona 在 Flink 内部以二进制方式存储几何对象，从而保持较低的内存占用与较高的处理速度。

执行空间连接时，Sedona 在底层使用空间索引，使查询能够快速完成。

容错由 Flink 的检查点（checkpoint）机制处理。如果某个节点崩溃，地理空间状态可以从最近一次检查点恢复。

通常的流程是：从 Kafka 或文件系统等数据源读取地理空间数据，使用 Sedona 的空间函数进行处理，然后将结果写入 Iceberg 等数据汇。

整条 SedonaFlink 管道持续运行，新事件会实时流过空间转换逻辑。

## 与其他方案的对比

对于较小的数据集，可能并不需要分布式集群，使用 SedonaDB 即可。

对于大规模批处理管道，可以使用 SedonaSpark。

下面是 SedonaFlink 与几种流式方案的直接对比。

**SedonaFlink 与 Sedona on Spark Structured Streaming**

Spark Streaming 采用微批（micro-batch）模式，而 Flink 是逐事件处理。在某些工作流中，这能让 Flink 的延迟更低。

Flink 的状态管理也更为成熟。

如果已经深度使用 Spark 生态，且 Spark Structured Streaming 的延迟可以满足需求，可以选择 Spark；如果对延迟有非常严苛的要求，建议选择 Flink。

**Sedona on Flink 与 PostGIS**

PostGIS 非常适合用于 OLTP 工作负载下的地理空间数据存储与查询，但它并不是为流式处理而设计的。

如果使用 PostGIS 处理流式工作负载，需要持续从流处理器中查询数据库，这会增加延迟，并对数据库造成压力。

SedonaFlink 在数据流转过程中即对地理空间数据进行处理，从而避免了与数据库之间的往返开销。
