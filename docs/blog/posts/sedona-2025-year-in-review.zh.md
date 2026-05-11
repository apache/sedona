---
date:
  created: 2026-01-11
links:
  - 发布说明: https://sedona.apache.org/latest/setup/release-notes/
  - SedonaDB: https://sedona.apache.org/sedonadb/
  - SpatialBench: https://sedona.apache.org/spatialbench/
  - Apache Parquet 和 Iceberg 原生 geo 类型: https://wherobots.com/blog/apache-iceberg-and-parquet-now-support-geo/
authors:
  - jia
title: "Apache Sedona 2025 年度回顾"
---

<!--
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
-->

2025 年对 **Apache Sedona** 而言是里程碑式的一年。我们在 Spark、Flink 和 Snowflake 上的分布式空间分析取得重大进展,推出了名为 SedonaDB 的全新单节点引擎,并推动了基准测试与开放地理空间数据标准的发展。

本文总结了 Apache Sedona 生态在 2025 年最重要的亮点。

<!-- more -->

## 2025 年 Apache Sedona 生态版本

Apache Sedona 在 2025 年 1 月到 2026 年 1 月间发布了四个版本:1.7.1、1.7.2、1.8.0 和 1.8.1。同年,Sedona 生态从两个重要方向得到扩展:我们推出了用于快速单机分析的 SedonaDB,以及让空间性能比较可复现的 SpatialBench。

- Apache Sedona 版本:在分布式引擎与集成(Spark、Flink、Snowflake)上的持续改进。详见发布说明。
- SedonaDB:为交互式分析和开发者工作流而生的全新单节点空间引擎。
- SpatialBench:一套用于标准化跨引擎空间 SQL 性能评估的基准测试套件。

发布说明:[https://sedona.apache.org/latest/setup/release-notes/](https://sedona.apache.org/latest/setup/release-notes/)

## 分布式引擎亮点

2025 年,SedonaSpark、SedonaFlink 和 SedonaSnow 都迎来了易用性的重大改进、更广泛的 SQL 覆盖,以及对现代开放地理空间数据格式的更好支持:

* SedonaSpark 上的 GeoPandas API:用 GeoPandas 风格的代码编写,通过 Sedona 在 Spark 上运行,让空间连接(`sjoin`)、缓冲、距离、坐标系变换等熟悉的工作流能够超越单机规模。了解更多:[Apache Sedona 的 GeoPandas API](../../tutorial/geopandas-api.md)。
* 用于聚类、异常检测和热点分析的 GeoStats:为 DataFrame 上常见的空间统计工作流提供内置工具,包括 DBSCAN 聚类、Local Outlier Factor(LOF)以及 Getis-Ord Gi/Gi* 热点分析。了解更多:[Stats 模块](../../api/stats/sql.md)。
* 借助 GeoArrow 加速 SedonaSpark 到 GeoPandas 的转换:通过 Arrow/GeoArrow 更高效地将查询结果转换为 GeoPandas,例如 `geopandas.GeoDataFrame.from_arrow(dataframe_to_arrow(df))`。了解更多:[GeoPandas + Shapely 互操作性](../../tutorial/geopandas-shapely.md)。
* STAC 目录读取器:使用 `sedona.read.format("stac")` 从本地文件、S3 或 HTTPS 端点加载 STAC 集合,并尽早应用时间/区域过滤以减少读取的数据量。同时支持需要认证的 STAC API。了解更多:[使用 Apache Sedona 和 Spark 处理 STAC 目录](../../tutorial/files/stac-sedona-spark.md)。
* 更多内置数据源:更轻松地接入实际常用的格式,包括 GeoPackage 和 OSM PBF(OpenStreetMap)。了解更多:[SedonaSQL / DataFrame I/O 教程](../../tutorial/sql.md)。
* 向量化 UDF(Python):通过 Apache Arrow 批量处理数据,以更快的方式运行 Python UDF,包括使用 Shapely 或 GeoPandas GeoSeries 的几何感知 UDF。了解更多:[Spatial vectorized UDFs (Python only)](../../tutorial/sql.md)。
* 跨引擎函数扩展:Spark、Flink 和 Snowflake 上的函数覆盖度持续扩展。例如:ST_ApproximateMedialAxis、ST_StraightSkeleton、ST_Collect_Agg 以及 ST_OrientedEnvelope。详见各引擎的函数目录:[SedonaSpark SQL](../../api/sql/Overview.md)、[SedonaFlink SQL](../../api/flink/Overview.md) 和 [SedonaSnow SQL](../../api/snowflake/vector-data/Overview.md)。

## SedonaDB:全新的单节点空间引擎

2025 年最重要的进展之一是推出了 SedonaDB——一款面向单机地理空间数据的全新分析引擎。

SedonaDB 于 2025 年 9 月公布,代表了 Sedona 项目家族的新方向。它使用 Rust 编写,基于 Apache Arrow 和 DataFusion 构建,提供快速的列式执行能力以及轻量化的部署模式。

SedonaDB 在 2025 年发布了两个版本:0.1.0(首版)和 0.2.0(重大扩展)。

首版 0.1.0 引入了核心引擎,内建原生几何和地理类型、空间索引以及优化过的空间连接与最近邻查询,同时提供 Python 和 SQL 接口,带来零配置、嵌入式的使用体验。

2025 年 12 月发布的 SedonaDB 0.2.0 迅速扩展了引擎能力,新增包括栅格在内的更广泛空间 SQL 覆盖、对 GDAL 和 OGR 兼容格式的原生读取支持、带边界框元数据的 GeoParquet 1.1 写入支持、Python UDF 支持以及初步的栅格数据类型支持。

博客文章:

* [SedonaDB 正式发布](https://sedona.apache.org/latest/blog/2025/09/24/introducing-sedonadb-a-single-node-analytical-database-engine-with-geospatial-as-a-first-class-citizen/)
* [SedonaDB 0.2.0 版本发布](https://sedona.apache.org/latest/blog/2025/12/01/sedonadb-020-release/)

## SpatialBench:标准化空间性能评估

2025 年另一个重要里程碑是推出 SpatialBench——一套专为空间 SQL 工作负载设计的基准测试套件。

传统的数据库基准测试常常忽略地理空间分析中最关键的模式,例如空间连接、距离过滤和空间聚合。SpatialBench 正是为填补这一空白而生。

SpatialBench 提供:

* 真实的空间数据集
* 可配置的规模因子
* 可复现的查询负载
* 跨引擎可比较的结果

SpatialBench 的首个版本评估了 SedonaDB、带空间扩展的 DuckDB 和 GeoPandas,提供了透明且可复现的性能对比。

博客文章:[SpatialBench 正式发布](https://sedona.apache.org/latest/blog/2025/12/11/introducing-spatialbench-performance-benchmarks-for-spatial-database-queries/)

## 推进开放地理空间数据格式

2025 年也是地理空间互操作性的转折点。Apache Iceberg 和 Apache Parquet 获得了原生的几何和地理类型支持,让在开放湖仓表中直接存储空间数据变得更轻松。

这一进步使得:

* 开放且厂商中立的空间存储
* 地理空间表的可靠事务
* 提前过滤数据,让引擎可以扫描更少
* 跨引擎的无缝互操作

Apache Sedona 与更广泛的地理空间社区都积极参与并推动了这项工作。

博客文章:[Apache Iceberg 和 Parquet 现已支持 Geo](https://wherobots.com/blog/apache-iceberg-and-parquet-now-support-geo/)

## 社区与生态增长

除技术里程碑之外,2025 年 Apache Sedona 社区也持续增长:

* 新的 committer 与贡献者加入项目
    - 新晋 committer:Pranav Toggi、Peter Nguyen、Dewey Dunnington
    - 新晋 PMC 成员:Feng Zhang
* 全年参与的贡献者更多
    - 2025 年,27 位新贡献者首次为 Apache Sedona 仓库(SedonaSpark、SedonaFlink、SedonaSnow)做出贡献,使项目总贡献者达到 155 人。整个 2025 年共有 46 人参与 Apache Sedona 的贡献。
    - 生态内的新贡献者:
        - SedonaDB:2025 年新增 26 位贡献者
        - SpatialBench:2025 年新增 8 位贡献者
* 项目采用持续增长
    - Apache Sedona 累计下载量已超过 6500 万次。
    - 月下载量已超过 200 万次。
    - 提交活动从 2024 年的 1,509 次提交增长到 2025 年的 2,137 次提交。

Sedona 已演进为一个多引擎、多部署形态的生态,既反映了社区的需求,也体现了贡献者们持续不断的努力。

## 展望 2026

凭借在分布式分析、单节点引擎、基准测试和开放格式上的强劲势头,Apache Sedona 进入 2026 年的状态非常良好,具备进一步增长的能力。

我们将继续关注以下方向:

* 更深入的栅格分析支持
* 更广的 SpatialBench 覆盖
* 与 Iceberg 原生空间特性的更紧密集成
* 在 Python、SQL 和 Rust 上更佳的开发者体验

空间分析正在成为现代数据平台的核心能力,Apache Sedona 在其中的基础项目地位也日益凸显。

感谢社区中所有为这丰收的 2025 年做出贡献的每一位。

## 参考资料

* Sedona 1.7.1、1.7.2、1.8.0 和 1.8.1 发布说明:[https://sedona.apache.org/latest/setup/release-notes/](https://sedona.apache.org/latest/setup/release-notes/)
* SedonaDB 0.1.0 发布说明:[https://sedona.apache.org/latest/blog/2025/09/24/introducing-sedonadb-a-single-node-analytical-database-engine-with-geospatial-as-a-first-class-citizen/](https://sedona.apache.org/latest/blog/2025/09/24/introducing-sedonadb-a-single-node-analytical-database-engine-with-geospatial-as-a-first-class-citizen/)
* SedonaDB 0.2.0 发布说明:[https://sedona.apache.org/latest/blog/2025/12/01/sedonadb-020-release/](https://sedona.apache.org/latest/blog/2025/12/01/sedonadb-020-release/)
* SpatialBench 发布说明:[https://sedona.apache.org/latest/blog/2025/12/11/introducing-spatialbench-performance-benchmarks-for-spatial-database-queries/](https://sedona.apache.org/latest/blog/2025/12/11/introducing-spatialbench-performance-benchmarks-for-spatial-database-queries/)
* Apache Parquet 和 Iceberg 原生 geo 类型:[https://wherobots.com/blog/apache-iceberg-and-parquet-now-support-geo/](https://wherobots.com/blog/apache-iceberg-and-parquet-now-support-geo/)
