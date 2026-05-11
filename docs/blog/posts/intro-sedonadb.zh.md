---
date:
  created: 2025-09-24
links:
  - SedonaDB: https://sedona.apache.org/sedonadb/
  - SpatialBench: https://sedona.apache.org/spatialbench/
authors:
  - dewey
  - kristin
  - feng
  - peter
  - jess
  - pranav
  - james
  - jia
  - matt_powers
  - kelly
title: "SedonaDB 正式发布:以地理空间为一等公民的单节点分析数据库引擎"
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

Apache Sedona 社区激动地宣布 [SedonaDB](https://sedona.apache.org/sedonadb) 首个版本正式发布! 🎉

SedonaDB 是首款将空间数据视为一等公民的开源单节点分析数据库引擎,作为 Apache Sedona 的子项目开发。

Apache Sedona 在 Spark(SedonaSpark)、Flink(SedonaFlink)和 Snowflake(SedonaSnow)等分布式引擎上提供大规模地理空间处理能力。SedonaDB 将 Sedona 生态系统扩展到单节点引擎,专为中小规模数据分析进行优化,提供分布式系统通常难以企及的简洁性与速度。

<!-- more -->

## 🤔 什么是 SedonaDB

SedonaDB 使用 Rust 编写,轻量、极速,并且原生支持空间数据。开箱即用,它提供:

* 🗺️ 在业界标准查询操作之上,全面支持空间类型、连接、CRS(坐标参考系统)和函数。
* ⚡ 查询优化、索引以及数据裁剪等底层特性,让空间操作天然高性能。
* 🐍 开发者熟悉的 Python 和 SQL 接口,并提供 R 与 Rust 的 API。
* ☁️ 可在单机环境下灵活运行于本地文件或数据湖之上。

SedonaDB 基于 Apache Arrow 和 Apache DataFusion 构建,具备现代向量化查询引擎所需的一切。它的独特之处在于能够原生处理空间负载,无需扩展或插件。安装非常简单,SedonaDB 可轻松集成到本地开发与云端管道中,提供跨环境一致的使用体验。

SedonaDB 的首个版本提供了一整套几何向量操作能力,并能与 GeoArrow、GeoParquet 和 GeoPandas 无缝集成。未来版本将支持所有流行的空间函数,包括针对栅格数据的函数。

## 🚀 SedonaDB 快速上手示例

首先安装 SedonaDB:

```
pip install "apache-sedona[db]"
```

然后实例化连接:

```python
import sedona.db

sd = sedona.db.connect()
```

让我们用 SedonaDB 执行一次空间连接。

假设你有一张 `cities` 表,其中包含表示每个城市中心的经纬度点,还有一张 `countries` 表,其中一列包含该国家地理边界的多边形。

下面是 `cities` 表中的几行数据:

```
┌──────────────┬───────────────────────────────┐
│     name     ┆            geometry           │
│   utf8view   ┆      geometry <epsg:4326>     │
╞══════════════╪═══════════════════════════════╡
│ Vatican City ┆ POINT(12.4533865 41.9032822)  │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ San Marino   ┆ POINT(12.4417702 43.9360958)  │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Vaduz        ┆ POINT(9.5166695 47.1337238)   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
```

下面是 `countries` 表中的几行数据:

```
┌─────────────────────────────┬───────────────┬────────────────────────────────────────────────────┐
│             name            ┆   continent   ┆                      geometry                      │
│           utf8view          ┆    utf8view   ┆                geometry <epsg:4326>                │
╞═════════════════════════════╪═══════════════╪════════════════════════════════════════════════════╡
│ Fiji                        ┆ Oceania       ┆ MULTIPOLYGON(((180 -16.067132663642447,180 -16.55… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ United Republic of Tanzania ┆ Africa        ┆ POLYGON((33.90371119710453 -0.9500000000000001,34… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Western Sahara              ┆ Africa        ┆ POLYGON((-8.665589565454809 27.656425889592356,-8… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
```

下面演示如何用一次空间连接计算每个城市所属的国家:

```python
sd.sql("""
select
    cities.name as city_name,
    countries.name as country_name,
    continent
from cities
join countries
where ST_Intersects(cities.geometry, countries.geometry)
""").show(3)
```

代码使用 `ST_Intersects` 来判断一个城市是否位于某个国家境内。

下面是查询的结果:

```
┌───────────────┬─────────────────────────────┬───────────┐
│   city_name   ┆         country_name        ┆ continent │
│    utf8view   ┆           utf8view          ┆  utf8view │
╞═══════════════╪═════════════════════════════╪═══════════╡
│ Suva          ┆ Fiji                        ┆ Oceania   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ Dodoma        ┆ United Republic of Tanzania ┆ Africa    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ Dar es Salaam ┆ United Republic of Tanzania ┆ Africa    │
└───────────────┴─────────────────────────────┴───────────┘
```

上述示例进行了一次点在多边形内(point-in-polygon)的连接,将城市位置(点)映射到它们所属的国家(多边形)。SedonaDB 利用空间索引(在合适的场景下)并基于输入数据样本在运行时动态调整连接策略,从而高效执行这些连接。许多通用引擎在这类操作的性能上表现挣扎,而 SedonaDB 是为空间负载量身打造的,始终能提供稳定的高性能结果。

## 📊 Apache Sedona SpatialBench

为了检验我们在 SedonaDB 上的工作,我们还需要一种机制来评估其性能与速度。为此我们开发了 Apache Sedona SpatialBench——一套用于评估数据库系统地理空间 SQL 分析查询性能的基准测试。

让我们对比 SedonaDB 与 GeoPandas、DuckDB Spatial 在几条典型空间查询上的表现,这些查询定义于 [SpatialBench](https://sedona.apache.org/spatialbench/)。

下面是 SpatialBench v0.1 在 Queries 1–12、规模因子 1(SF1)和规模因子 10(SF10)下的结果。

![Scale Factor 1 benchmark results](../../image/blog/sedonadb1/sf1-09242025.png){ width="400" }
![Scale Factor 10 benchmark results](../../image/blog/sedonadb1/sf10-09242025.png){ width="400" }
{: .grid }

SedonaDB 在各种查询类型上都有均衡的表现,并能有效扩展到 SF 10。DuckDB 在空间过滤和某些几何操作上表现出色,但在复杂连接和 KNN 查询上面临挑战。GeoPandas 虽然在 Python 生态中流行,但要有效处理更大数据集需要手工优化和并行化。完整的性能分析请参见 [SpatialBench 网站](https://sedona.apache.org/spatialbench/single-node-benchmarks/)。

下面是 SpatialBench Query #8 的示例,它在 SedonaDB 和 DuckDB 上都可运行:

```sql
SELECT b.b_buildingkey, b.b_name, COUNT(*) AS nearby_pickup_count
FROM trip t JOIN building b ON ST_DWithin(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary), 0.0045) -- ~500m
GROUP BY b.b_buildingkey, b.b_name
ORDER BY nearby_pickup_count DESC
```

这个查询特意在点和多边形之间执行了一次基于距离的空间连接,然后对结果进行聚合。

下面是查询的输出:

```
┌───────────────┬──────────┬─────────────────────┐
│ b_buildingkey ┆  b_name  ┆ nearby_pickup_count │
│     int64     ┆ utf8view ┆        int64        │
╞═══════════════╪══════════╪═════════════════════╡
│          3779 ┆ linen    ┆                  42 │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│         19135 ┆ misty    ┆                  36 │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│          4416 ┆ sienna   ┆                  26 │
└───────────────┴──────────┴─────────────────────┘
```

下面是等效的 GeoPandas 代码:

```python
trips_df = pd.read_parquet(data_paths["trip"])
trips_df["pickup_geom"] = gpd.GeoSeries.from_wkb(
    trips_df["t_pickuploc"], crs="EPSG:4326"
)
pickups_gdf = gpd.GeoDataFrame(trips_df, geometry="pickup_geom", crs="EPSG:4326")

buildings_df = pd.read_parquet(data_paths["building"])
buildings_df["boundary_geom"] = gpd.GeoSeries.from_wkb(
    buildings_df["b_boundary"], crs="EPSG:4326"
)
buildings_gdf = gpd.GeoDataFrame(
    buildings_df, geometry="boundary_geom", crs="EPSG:4326"
)

threshold = 0.0045  # degrees (~500m)
result = (
    buildings_gdf.sjoin(pickups_gdf, predicate="dwithin", distance=threshold)
    .groupby(["b_buildingkey", "b_name"], as_index=False)
    .size()
    .rename(columns={"size": "nearby_pickup_count"})
    .sort_values(["nearby_pickup_count", "b_buildingkey"], ascending=[False, True])
    .reset_index(drop=True)
)
```

## 🗺️ SedonaDB 的 CRS 管理

SedonaDB 在读写文件以及处理 DataFrame 时都会管理 CRS,让你的管道更安全,减少手动工作。

让我们计算佛蒙特州的建筑数量,以展示 SedonaDB 内置的 CRS 管理特性。

先用 GeoPandas 读取一个使用 EPSG 32618 CRS 的 FlatGeobuf 文件,然后将其转换为 SedonaDB DataFrame:

```python
import geopandas as gpd

path = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/example-crs/files/example-crs_vermont-utm.fgb"
gdf = gpd.read_file(path)
vermont = sd.create_data_frame(gdf)
```

让我们检查 `vermont` DataFrame 的 schema:

```
vermont.schema

SedonaSchema with 1 field:
  geometry: wkb <epsg:32618>
```

可以看到 `vermont` DataFrame 保留了 FlatGeobuf 文件中指定的 CRS。SedonaDB 还没有原生的 FlatGeobuf 读取器,但用 GeoPandas 的 FlatGeobuf 读取器再用一行代码转换为 SedonaDB DataFrame 也非常方便。

现在将一个 GeoParquet 文件读入 SedonaDB DataFrame。

```python
buildings = sd.read_parquet(
    "https://github.com/geoarrow/geoarrow-data/releases/download/v0.2.0/microsoft-buildings_point_geo.parquet"
)
```

检查 DataFrame 的 schema:

```
buildings.schema

SedonaSchema with 1 field:
  geometry: geometry <ogc:crs84>
```

让我们把这两张表注册为视图,并运行一次空间连接,看看有多少建筑物位于佛蒙特州:

```python
buildings.to_view("buildings", overwrite=True)
vermont.to_view("vermont", overwrite=True)

sd.sql("""
select count(*) from buildings
join vermont
where ST_Intersects(buildings.geometry, vermont.geometry)
""").show()
```

这条命令会正确报错,因为两张表的 CRS 不同。出于安全考虑,SedonaDB 宁可报错也不会给出错误的答案!错误信息也很容易调试:

```
SedonaError: type_coercion
caused by
Error during planning: Mismatched CRS arguments: ogc:crs84 vs epsg:32618
Use ST_Transform() or ST_SetSRID() to ensure arguments are compatible.
```

让我们改写这次空间连接,将 `vermont` 的 CRS 转换为 EPSG:4326,使其与 `buildings` 的 CRS 兼容。

```python
sd.sql("""
select count(*) from buildings
join vermont
where ST_Intersects(buildings.geometry, ST_Transform(vermont.geometry, 'EPSG:4326'))
""").show()
```

现在就能得到正确结果!

```
┌──────────┐
│ count(*) │
│   int64  │
╞══════════╡
│   361856 │
└──────────┘
```

SedonaDB 在读写文件、与 GeoPandas DataFrame 互转或进行 DataFrame 操作时都会跟踪 CRS,让你的空间计算安全且正确!

## 🎯 SedonaDB 的真实示例

下面来看一个更复杂的空间操作:KNN 连接。

假设你正在分析共享出行数据,并希望确定哪些建筑物最常出现在上车点附近——这有助于理解出行起点与附近的地标、商家或居住建筑之间的关系,这些可能会影响乘车需求的模式。

这个查询用空间最近邻分析,为每个出行上车点找出最近的 5 栋建筑物。对于每次出行,它都会识别出地理上最接近上车点的 5 栋建筑物,并计算到每栋建筑物的精确距离。

查询如下:

```sql
WITH trip_with_geom AS (
    SELECT t_tripkey, t_pickuploc, ST_GeomFromWKB(t_pickuploc) as pickup_geom
    FROM trip
),
building_with_geom AS (
    SELECT b_buildingkey, b_name, b_boundary, ST_GeomFromWKB(b_boundary) as boundary_geom
    FROM building
)
SELECT
    t.t_tripkey,
    t.t_pickuploc,
    b.b_buildingkey,
    b.b_name AS building_name,
    ST_Distance(t.pickup_geom, b.boundary_geom) AS distance_to_building
FROM trip_with_geom t JOIN building_with_geom b
ON ST_KNN(t.pickup_geom, b.boundary_geom, 5, FALSE)
ORDER BY distance_to_building ASC, b.b_buildingkey ASC
```

查询结果如下:

```
┌───────────┬───────────────────────────────┬───────────────┬───────────────┬──────────────────────┐
│ t_tripkey ┆          t_pickuploc          ┆ b_buildingkey ┆ building_name ┆ distance_to_building │
│   int64   ┆             binary            ┆     int64     ┆      utf8     ┆        float64       │
╞═══════════╪═══════════════════════════════╪═══════════════╪═══════════════╪══════════════════════╡
│   5854027 ┆ 01010000001afa27b85825504001… ┆            79 ┆ gainsboro     ┆                  0.0 │
├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│   3326828 ┆ 01010000001bfcc5b8b7a95d4083… ┆           466 ┆ deep          ┆                  0.0 │
├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│   1239844 ┆ 0101000000ce471770d6ce2a40f9… ┆           618 ┆ ivory         ┆                  0.0 │
└───────────┴───────────────────────────────┴───────────────┴───────────────┴──────────────────────┘
```

这是 [SpatialBench](https://github.com/apache/sedona-spatialbench/) 中的一条查询。

## 🦀 为什么 SedonaDB 用 Rust 构建

SedonaDB 用 Rust 构建——这是一门高性能、内存安全的语言,提供细粒度的内存管理和成熟的数据库库生态。它充分利用了这一生态,集成了 [Apache DataFusion](https://github.com/apache/datafusion)、[GeoArrow](https://github.com/geoarrow/geoarrow) 和 [georust/geo](https://github.com/georust/geo) 等项目。

虽然 Spark 提供了扩展点,使 SedonaSpark 能在分布式场景下优化空间查询,但 DataFusion 在单节点上提供了用于裁剪、空间算子和优化器规则的稳定 API。这让我们能在引擎内部嵌入深度的空间感知能力,同时保留完整的非空间功能。感谢 DataFusion 项目及其社区,这一切才得以实现且过程愉快。

## ⚖️ 为什么同时需要 SedonaDB 和 SedonaSpark

SedonaSpark 适用于大规模地理空间负载,或者你的生产环境已经在使用 Spark。例如,在 100 GB 矢量数据集与大型栅格数据集之间进行连接。但对于较小的数据集,Spark 的分布式架构会带来不必要的开销,让本地运行更慢、安装更困难、调优更棘手。

SedonaDB 更适合较小的数据集和本地计算场景。SedonaDB 的空间函数与 SedonaSpark 的函数兼容,因此适用于其中一个引擎的 SQL 片段通常也能在另一个引擎上运行。我们会持续推动两个项目 API 的全面互操作。下面这段用于分析 Overture 建筑表的代码片段可以同时在两个引擎上运行。

```
nyc_bbox_wkt = (
    "POLYGON((-74.2591 40.4774, -74.2591 40.9176, -73.7004 40.9176, -73.7004 40.4774, -74.2591 40.4774))"
)

sd.sql(f"""
SELECT
    id,
    height,
    num_floors,
    roof_shape,
    ST_Centroid(geometry) as centroid
FROM
    buildings
WHERE
    is_underground = FALSE
    AND height IS NOT NULL
    AND height > 20
    AND ST_Intersects(geometry, ST_SetSRID(ST_GeomFromText('{nyc_bbox_wkt}'), 4326))
LIMIT 5;
```

## 🚀 下一步

虽然 SedonaDB 已经经过充分测试,并提供了一组能完成多种空间分析的核心特性,但它仍是一个早期项目,有许多新功能的拓展空间。

许多 ST 函数有待补齐。其中一些相对直接,另一些则相当复杂。

社区还会为 SedonaDB 内建更多空间文件格式(如 GeoPackage 和 GeoJSON)的支持。在此之前,你可以先将这些格式的数据读入 GeoPandas DataFrame,再转换为 SedonaDB DataFrame。

栅格支持也在路线图中,这是一个复杂的工程,如果你对用 Rust 解决有挑战的问题感兴趣,这正是绝佳的贡献机会。

更多关于下一个版本任务的细节请参考 [SedonaDB v0.2 里程碑](https://github.com/apache/sedona-db/milestone/1)。也欢迎你创建 issue、在 Discord 留言或在 GitHub discussions 上头脑风暴新功能。

## 🤝 加入社区

Apache Sedona 社区有活跃的 Discord 社区、每月用户会议和定期的贡献者会议。

SedonaDB 欢迎来自社区的贡献。如有需要,你可以申请认领某个 issue,我们会很乐意分配给你。也欢迎你加入贡献者会议,其他活跃的贡献者会乐意帮助你完成 pull request!

!!! info
    我们将以一场特别的 Apache Sedona 社区办公时间庆祝 SedonaDB 与 SpatialBench 的发布!

    📅 2025 年 10 月 7 日

    ⏰ 太平洋时间上午 8–9 点

    📍 线上

    🔗 [在此报名](https://bit.ly/3UBmxFY)
