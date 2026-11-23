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

## 使用方式

SedonaSQL 支持多种参数。修改它们的取值有以下几种方式：

1. 通过 SparkConf 设置：

```scala
sparkSession = SparkSession.builder().
      config("spark.serializer","org.apache.spark.serializer.KryoSerializer").
      config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator").
      config("sedona.global.index","true")
      master("local[*]").appName("mySedonaSQLdemo").getOrCreate()
```

2. 查看当前的 SedonaSQL 配置：

```scala
val sedonaConf = new SedonaConf(sparkSession.conf)
println(sedonaConf)
```

3. Sedona 参数可以在运行时动态修改：

```scala
sparkSession.conf.set("sedona.global.index","false")
```

此外，你也可以在参数名前添加 `spark` 前缀，例如：

```scala
sparkSession.conf.set("spark.sedona.global.index","false")
```

不过，所有通过 `spark` 前缀设置的参数都会被 Spark 所识别，这意味着你可以提前通过 `spark-defaults.conf` 或 Spark on Kubernetes 的配置来设定这些参数。

如果同一个参数同时通过 `sedona` 和 `spark.sedona` 两种前缀进行设置，则 `sedona` 前缀设置的值会覆盖 `spark.sedona` 前缀设置的值。

## 通用参数

| 参数 | 说明 | 默认值 | 可选值 |
| :--- | :--- | :--- | :--- |
| `sedona.global.index` | 是否启用空间索引（目前仅支持 SQL 范围连接与 SQL 距离连接） | `true` | `true`, `false` |
| `sedona.global.indextype` | 空间索引类型，仅在 `sedona.global.index` 为 true 时生效 | `rtree` | `rtree`, `quadtree` |
| `spark.sedona.enableParserExtension` | 启用解析器扩展，用于在 SQL DDL 语句中解析 GEOMETRY 数据类型 | `true` | `true`, `false` |
| `spark.sedona.function.prefix` | 为 Sedona 注册的每个 SQL 函数名添加的前缀。设置为 `sedona_` 时，`ST_Buffer` 只会注册为 `sedona_ST_Buffer`，不带前缀的原名留给平台使用。请在集群或服务端的 Spark 配置中设置：同一个 JVM 中第一个 Sedona 会话会确定该 JVM 的取值。通过 `spark.sql.extensions` 加载 Sedona 时，Sedona 只从该配置读取取值，并忽略在会话上设置的值。自 v2.0.0 起 | _（为空：不加前缀）_ | 字母、数字和下划线，且以字母或下划线开头。如果加上前缀后某个 Sedona 函数名会变成另一个 Sedona 函数名，Sedona 会拒绝该前缀。例如 `ST_` 会被拒绝：`GeometryType` 会变成 `ST_GeometryType`。 |

## 连接（Join）相关参数

| 参数 | 说明 | 默认值 | 可选值 |
| :--- | :--- | :--- | :--- |
| `sedona.join.autoBroadcastJoinThreshold` | 执行连接时，将被广播到所有工作节点的表的最大字节大小。设为 -1 可禁用自动广播。 | 与 `spark.sql.autoBroadcastJoinThreshold` 相同 | 带有字节后缀的整数，例如 `10MB` 或 `512KB` |
| `sedona.join.gridtype` | 连接查询使用的空间分区网格类型 | `kdbtree` | `quadtree`, `kdbtree` |
| `spark.sedona.join.knn.includeTieBreakers` | KNN 连接结果是否包含所有并列项，可能返回多于 k 个结果 | `false` | `true`, `false` |
| `sedona.join.indexbuildside` | **（高级）** Sedona 在哪一侧构建空间索引 | `left` | `left`, `right` |
| `sedona.join.numpartition` | **（高级）** 连接查询双方的分区数 | `-1`（使用现有分区数） | 任意整数 |
| `sedona.join.spatitionside` | **（高级）** 空间分区阶段的主导侧 | `left` | `left`, `right` |
| `sedona.join.optimizationmode` | **（高级）** Sedona 何时优化空间连接 SQL 查询 | `nonequi` | `all`（始终优化，包括等值连接），`none`（禁用优化），`nonequi`（仅优化非等值连接） |

## CRS 转换参数

| 参数 | 说明 | 默认值 | 可选值 | 起始版本 |
| :--- | :--- | :--- | :--- | :--- |
| `spark.sedona.crs.geotools` | 控制 ST_Transform 中 CRS 转换所使用的库 | `raster` | `none`（全部使用 proj4sedona），`raster`（矢量使用 proj4sedona，栅格使用 GeoTools），`all`（全部使用 GeoTools —— 旧版行为） | v1.9.0 |
| `spark.sedona.crs.url.base` | CRS 定义服务器的基础 URL，用于通过 HTTP 解析权威机构代码（如 EPSG）。设置后，ST_Transform 会先查询该 URL 提供者，再回退到内置定义。 | _（空 —— 禁用）_ | 例如 `https://crs.example.com` | v1.9.0 |
| `spark.sedona.crs.url.pathTemplate` | 附加在 `spark.sedona.crs.url.base` 之后的 URL 路径模板。占位符 `{authority}` 和 `{code}` 会在运行时被替换。 | `/{authority}/{code}.json` | 例如 `/epsg/{code}.json` | v1.9.0 |
| `spark.sedona.crs.url.format` | URL 提供者返回的 CRS 定义格式 | `projjson` | `projjson`、`proj`、`wkt1`（OGC WKT1）、`wkt2`（ISO 19162 WKT2） | v1.9.0 |

## GeoParquet 写出选项

以下是 `geoparquet` 写出操作的选项，而不是会话参数，需要在 writer 上设置：

```scala
df.write.format("geoparquet")
    .option("geoparquet.version", "2.0.0")
    .option("geoparquet.crs.geometry", projjson)
    .save("/path/to/saved_geoparquet.parquet")
```

| 选项 | 说明 | 默认值 | 可选值 | 起始版本 |
| :--- | :--- | :--- | :--- | :--- |
| `geoparquet.version` | 文件的 GeoParquet 版本。`2.0.0` 版本把几何列存为 Parquet 的 `GEOMETRY` 逻辑类型，需要 Spark 4.1 及以上版本。 | `1.1.0` | `1.0.0`、`1.1.0`、`2.0.0` | v1.5.1（`2.0.0` 自 v2.0.0 起） |
| `geoparquet.crs` | 所有几何列的 CRS | 由几何对象的 SRID 推导 | PROJJSON 字符串、`""`（OGC:CRS84）或 `"null"`（未知 CRS） | v1.5.1 |
| `geoparquet.crs.<column_name>` | 单个几何列的 CRS，优先于 `geoparquet.crs`。 | 由几何对象的 SRID 推导 | PROJJSON 字符串、`""`（OGC:CRS84）或 `"null"`（未知 CRS） | v1.5.1 |
| `geoparquet.covering` | 几何列的边界框列，适用于只有一个几何列的 DataFrame | 见 `geoparquet.covering.mode` | 包含 `xmin`、`ymin`、`xmax`、`ymax` 的顶层 struct 列的名称 | v1.6.1 |
| `geoparquet.covering.<column_name>` | 单个几何列的边界框列 | 见 `geoparquet.covering.mode` | 包含 `xmin`、`ymin`、`xmax`、`ymax` 的顶层 struct 列的名称 | v1.6.1 |
| `geoparquet.covering.mode` | 写出 GeoParquet `1.1.0` 时，如何处理没有 covering 选项的几何列 | `auto` | `auto`（以 `<column_name>_bbox` 作为边界框列，DataFrame 中没有该列时自动生成）、`legacy`（不写出 covering） | v1.9.0 |
| `geoparquet.srid.lookahead.bytes` | 在 Spark 4.1 上写出 GeoParquet `2.0.0` 时，没有 CRS 选项的几何列的 CRS 取自每个文件中该列的第一个几何对象。本选项是在所有这类列都出现几何对象之前，每个文件暂存在内存中的行的大小上限。 | `16777216`（16 MB） | 字节数 | v2.0.0 |

自 v2.0.0 起，选项的键不区分大小写，与 Spark 处理写出选项的键的方式一致。对于以列名结尾的键，先按键中的写法与几何列的名称比较：`geoparquet.covering.MODE` 是几何列 `MODE` 的边界框列。如果没有几何列的名称与该写法完全一致，再忽略大小写进行比较。此时如果有多个几何列匹配，写出会失败；在 `spark.sql.caseSensitive=true` 时 DataFrame 可能包含这样的列。

示例请参见 [在 Spark 上使用 Apache Sedona 处理 GeoParquet](../../tutorial/files/geoparquet-sedona-spark.md)。
