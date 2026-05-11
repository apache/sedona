---
date:
  created: 2026-03-01
links:
  - SedonaDB: https://sedona.apache.org/sedonadb/
authors:
  - dewey
  - kristin
  - feng
  - peter
  - jia
  - pranav
title: "SedonaDB 0.3.0 版本发布"
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

Apache Sedona 社区激动地宣布
[SedonaDB](https://sedona.apache.org/sedonadb) 0.3.0 版本正式发布!

SedonaDB 是首款将空间数据视为一等公民的开源单节点分析数据库引擎,作为
Apache Sedona 的子项目开发。本次版本包含
[187 个已解决的 issue](https://github.com/apache/sedona-db/milestone/2?closed=1),
来自 18 位贡献者,并新增 36 个函数。

Apache Sedona 在 Spark(SedonaSpark)、Flink(SedonaFlink)和 Snowflake
(SedonaSnow)等分布式引擎上提供大规模地理空间处理能力。SedonaDB 将 Sedona
生态系统扩展到单节点引擎,专为中小规模数据分析进行优化,提供分布式系统
通常难以企及的简洁性与速度。

<!-- more -->

## 版本亮点

本次版本中有许多值得一提的亮点!

- 超出内存大小的空间连接
- 行级/单要素级 CRS 支持
- 参数化 SQL 查询
- Parquet 读取器和写入器改进
- 支持 GDAL/OGR 写入
- 空间函数覆盖度与文档改进
- R DataFrame API

```python
# pip install "apache-sedona[db]"
import sedona.db

sd = sedona.db.connect()
sd.options.interactive = True
```

## 超出内存大小的空间连接

四个月前发布的首个 SedonaDB 版本就已经包含了一个极为灵活且高性能的空间
连接。简单回顾一下,空间连接根据某种空间关系(例如相交或距离范围内)
找出两张表之间的交互。例如,如果你打算搬家并且喜欢披萨,SedonaDB 几乎能
在你的网络/硬盘加载 [Overture Maps
Divisions](https://docs.overturemaps.org/guides/divisions/) 和
[Places](https://docs.overturemaps.org/guides/places/) 数据的速度下,
为你找出披萨店最多的街区。

```python
overture = "s3://overturemaps-us-west-2/release/2026-02-18.0"
options = {"aws.skip_signature": True, "aws.region": "us-west-2"}
sd.read_parquet(f"{overture}/theme=places/type=place/", options=options).to_view(
    "places"
)
sd.read_parquet(
    f"{overture}/theme=divisions/type=division_area/", options=options
).to_view("divisions")

sd.sql("""
    SELECT
        get_field(names, 'primary') AS name,
        geometry
    FROM places
    WHERE get_field(categories, 'primary') = 'pizza_restaurant'
""").to_view("pizza_restaurants")

sd.sql("""
    SELECT divisions.id, get_field(divisions.names, 'primary') AS name, COUNT(*) AS n
    FROM divisions
    INNER JOIN pizza_restaurants ON ST_Contains(divisions.geometry, pizza_restaurants.geometry)
    WHERE divisions.subtype = 'neighborhood'
    GROUP BY divisions.id, get_field(divisions.names, 'primary')
    ORDER BY n DESC
""").limit(5)
# > ┌──────────────────────────────────────┬────────────────────────────┬───────┐
# > │                  id                  ┆            name            ┆   n   │
# > │                 utf8                 ┆            utf8            ┆ int64 │
# > ╞══════════════════════════════════════╪════════════════════════════╪═══════╡
# > │ 1373e423-efdc-471a-ae51-3e9d6c4231b2 ┆ UPZs de Bogotá             ┆   860 │
# > ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
# > │ 74e87dad-3b11-4fc5-bedd-cb51a617e141 ┆ Distrito-sede de Guarulhos ┆   388 │
# > ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
# > │ 76077e78-c0c0-48f4-9683-1a16a56dfaf9 ┆ Roma I                     ┆   346 │
# > ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
# > │ ee274c81-5b62-4e80-be67-e613b9219b17 ┆ Roma VII                   ┆   289 │
# > ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
# > │ bf762f01-cad0-46be-b47b-76435478035c ┆ Birmingham                 ┆   263 │
# > └──────────────────────────────────────┴────────────────────────────┴───────┘
```

这个连接算法大致流程是:(1) 将空间连接其中一侧的所有批次读入内存,
(2) 在内存中构建空间索引,(3) 对连接另一侧的每个批次并行查询索引,
并以流式方式返回结果。大多数空间 DataFrame 库的连接都是这么做的;
所不同的是,SedonaDB 还能利用 DataFusion 的并行流式执行基础设施,以及
Apache Arrow 对几何数组的紧凑表示,高效执行普通用户能想到的大多数连接。

虽然初版的连接已经足够快,但它有一个硬性上限:构建侧未压缩的所有批次
以及空间索引必须能放入内存。对于一台中端、内存 16 GB 的笔记本而言,
这大约相当于 2 亿个点(还没算非几何列所需的空间)——对大多数分析够用,
但对于大数据集或资源受限的环境(如容器或云端部署)则不够。

此外,我们还有一项独特的资源可用:Apache Sedona 社区!Sedona Spark
已经能进行分布式空间连接多年,其作者(Jia Yu、Kristin Cowalcijk 和
Feng Zhang)都是活跃的 SedonaDB 贡献者。在 0.2 发布之后我们提出了一个
问题:能否将 Sedona Spark 空间连接背后的思想搬过来,让 SedonaDB 可以
连接……*任何东西*?

最终的成果是几乎重写过的空间连接实现(包括 k 近邻或 KNN 连接),
让 SedonaDB 用户真正可以无所畏惧:只要你能写出查询,SedonaDB 就能完成任务。
这是一项足够复杂的特性,后续会有专门的博客文章介绍。这里先给一个简单
示例:SedonaDB 可以在仅 3 GB 内存的情况下,大约 30 秒内识别出一个
包含 1.3 亿个点的数据集中重复(或近似重复)的点!

```python
import sedona.db

sd = sedona.db.connect()
sd.options.memory_limit = "3g"
sd.options.memory_pool_type = "fair"

url = "https://github.com/geoarrow/geoarrow-data/releases/download/v0.2.0/microsoft-buildings_point.parquet"
sd.read_parquet(url).to_view("buildings")
sd.sql("""
    SELECT ROW_NUMBER() OVER () AS building_id, geometry
    FROM buildings
    """).to_parquet("buildings_idx.parquet")

sd.sql("""
    SELECT
        l.building_id,
        r.building_id AS nearest_building_id,
        l.geometry AS geometry,
        ST_Distance(l.geometry, r.geometry) AS dist
    FROM "buildings_idx.parquet" AS l
    JOIN "buildings_idx.parquet" AS r
        ON l.building_id <> r.building_id
        AND ST_DWithin(l.geometry, r.geometry, 1e-10)
    """).to_memtable().to_view("duplicates", overwrite=True)

sd.view("duplicates").count()
# > 1017476
```

作为参考,DuckDB 1.5.0(beta)需要约 12 GB 内存才能完成同样的工作,
GeoPandas 至少需要 28 GB。

对于这件事所付出的工作量,光说"感谢"完全不够,我们只能告诉你
[@Kontinuation](https://github.com/Kontinuation) 几乎独自设计并实现了
这一切,希望你能给予他应有的钦佩。

## 行级/单要素级 CRS 支持

自首个版本起,SedonaDB 就遵循 GeoParquet、GeoPandas 等空间 DataFrame
库的惯例支持坐标参考系统(CRS):每一列可以可选地声明一个 CRS,
说明每个值的坐标如何与地球表面对应[^1]。这样做的好处在于将 CRS 的考量
摊销到每个查询或每个批次一次;然而,这种模式让 SedonaDB 在与 PostGIS、
Sedona Spark 等允许每个要素声明自己 CRS 的系统交互时显得别扭。
单要素级 CRS 也有其他用处,例如在基于米的本地 CRS 中执行计算,或从
多个来源读入数据后再让 SedonaDB 将它们转换到统一的 CRS。

单要素级 CRS 支持是在 SedonaDB 0.2 中加入的,但在 0.3 版本中我们将其
贯穿整个技术栈,使得这种类型的列在所有函数中都能正常工作。例如,
如果你想把某些输入转换到本地 CRS,SedonaDB 可以做到:

```python
import pandas as pd

cities = pd.DataFrame(
    {
        "name": ["Ottawa", "Vancouver", "Toronto"],
        "utm_code": ["EPSG:32618", "EPSG:32610", "EPSG:32617"],
        "srid": [32618, 32610, 32617],
        "longitude": [-75.7019612, -123.1235901, -79.38945855491194],
        "latitude": [45.4186427, 49.2753624, 43.66464454743429],
    }
)

sd.create_data_frame(cities).to_view("cities", overwrite=True)
sd.sql("""
  SELECT
    name,
    ST_Transform(
      ST_Point(longitude, latitude, 4326),
      utm_code
    ) AS geom
  FROM cities
  """).to_view("cities_item_crs")

sd.view("cities_item_crs")
```

    ┌───────────┬───────────────────────────────────────────────────────────────────────┐
    │    name   ┆                                  geom                                 │
    │    utf8   ┆                                 struct                                │
    ╞═══════════╪═══════════════════════════════════════════════════════════════════════╡
    │ Ottawa    ┆ {item: POINT(445079.1082827766 5029697.641232558), crs: EPSG:32618}   │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Vancouver ┆ {item: POINT(491010.24691805616 5458074.5942144925), crs: EPSG:32610} │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Toronto   ┆ {item: POINT(629849.6255738225 4835886.955149793), crs: EPSG:32617}   │
    └───────────┴───────────────────────────────────────────────────────────────────────┘

最关键的是,它还能转换回来!

```python
sd.sql("SELECT name, ST_Transform(geom, 4326) FROM cities_item_crs")
```

    ┌───────────┬────────────────────────────────────────────────┐
    │    name   ┆ st_transform(cities_item_crs.geom,Int64(4326)) │
    │    utf8   ┆                    geometry                    │
    ╞═══════════╪════════════════════════════════════════════════╡
    │ Ottawa    ┆ POINT(-75.7019612 45.4186427)                  │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Vancouver ┆ POINT(-123.1235901 49.275362399999985)         │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Toronto   ┆ POINT(-79.38945855491194 43.664644547434285)   │
    └───────────┴────────────────────────────────────────────────┘

创建单要素级 CRS 的主要途径是 `ST_Transform()` 和
`ST_SetSRID()`,以及 `ST_Point()`、`ST_GeomFromWKT()`、`ST_GeomFromWKB()`
的最后一个参数。新增的 `ST_GeomFromEWKT()` 和 `ST_GeomFromEWKB()`
始终返回单要素级 CRS(并且在通过 `ST_AsEWKT()` 和 `ST_AsEWKB()` 导出时
SRID 会被保留)。单要素级 CRS 应当在所有其他函数调用中得到保留
(就像之前版本中类型级 CRS 那样)。我们很期待听到更多用例,以确保
我们的支持覆盖到真实世界中各种使用场景!

## 参数化查询

虽然 SedonaDB 的 Python 绑定提供了有限的 DataFrame API,但到目前为止与
SedonaDB 进行实质性交互的主要方式仍是 SQL。SQL 受到 LLM 良好支持,
也让我们能利用 DataFusion 出色的 SQL 解析器;但在编程环境中与 SedonaDB
交互就会变得别扭,完全依赖将查询参数序列化到 SQL 中。

在 SedonaDB 0.3.0 中,我们在 R 和 Python 中添加了参数化查询支持,并为
大多数类几何对象和/或类 CRS 对象提供了转换器。例如,如果你的上下文
信息是一个 GeoPandas DataFrame,你现在可以将它插入到任何 SQL 查询中,
而无需担心设置 CRS 或将其序列化为 SQL。

```python
import geopandas

url_countries = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_countries.fgb"
countries = geopandas.read_file(url_countries)
canada = countries.geometry[countries.name == "Canada"]

url_cities = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities.fgb"
sd.read_pyogrio(url_cities).to_view("cities", overwrite=True)
sd.sql("SELECT * FROM cities WHERE ST_Contains($1, wkb_geometry)", params=(canada,))
```

    ┌───────────┬─────────────────────────────────────────────┐
    │    name   ┆                 wkb_geometry                │
    │    utf8   ┆                   geometry                  │
    ╞═══════════╪═════════════════════════════════════════════╡
    │ Ottawa    ┆ POINT(-75.7019612 45.4186427)               │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Vancouver ┆ POINT(-123.1235901 49.2753624)              │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Toronto   ┆ POINT(-79.38945855491194 43.66464454743429) │
    └───────────┴─────────────────────────────────────────────┘

## Parquet 改进

SedonaDB 首次发布时,Parquet 的 Geometry 和 Geography 类型才[刚被加入
Parquet 规范](https://cloudnativegeo.org/blog/2025/02/geoparquet-2.0-going-native/),生态中的支持也很有限。
从 SedonaDB 0.3.0 起,定义了几何和地理列的 Parquet 文件已可开箱即用!
这意味着 SedonaDB 不再需要额外的(bbox)列和 GeoParquet 元数据就能
高效查询一个 Parquet 文件:内置的几何和地理类型自带嵌入式统计信息,
许多应用都能因此减小文件体积。

```python
# A Parquet file with no GeoParquet metadata
url = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_countries.parquet"
sd.read_parquet(url).schema
```

    SedonaSchema with 3 fields:
      name: utf8<Utf8View>
      continent: utf8<Utf8View>
      geometry: geometry<WkbView(ogc:crs84)>

虽然 SedonaDB 现在可以读取此类文件,但还不能写出它们(请期待 0.4.0!)。
有关 Parquet 中空间类型的更多内容,请参见 [Parquet 博客上最近的深入文章](https://parquet.apache.org/blog/2026/02/13/native-geospatial-types-in-apache-parquet/)。

除了读取改进,我们还改进了 Parquet 的写入接口。具体来说是写出对 SedonaDB、
GeoPandas 以及其他引擎的空间过滤下推都更友好的 GeoParquet 1.1 文件。
搭配 SedonaDB 的 GDAL/OGR 读取支持,可以从几乎任何来源生成优化的
GeoParquet 文件:

```python
url = "https://github.com/geoarrow/geoarrow-data/releases/download/v0.2.0/ns-water_water-point.fgb"
sd.read_pyogrio(url).to_parquet(
    "optimized.parquet",
    geoparquet_version="1.1",
    max_row_group_size=100_000,
    sort_by="wkb_geometry",
)
```

## GDAL/OGR 写入支持

在 SedonaDB 0.2.0 中,我们借助出色的 GDAL/OGR 以及 [pyogrio
包](https://github.com/geopandas/pyogrio) 的 Arrow 接口,增加了对
Shapefile、FlatGeoBuf、GeoPackage 等多种空间文件格式的读取支持。
在 0.3.0 中,我们又加入了写入支持!由于读取和写入都是流式的,
这让 SedonaDB 成为任意规模输入下读取/写入/转换工作流的优秀选择。

```python
sd.read_parquet("optimized.parquet").to_pyogrio("water-point.fgb")
```

## 空间函数覆盖度提升

自 0.2.0 发布以来,我们很荣幸与众多贡献者合作,为不断壮大的函数库
新增了 36 个 `ST_` 和 `RS_` 函数。rs_bandpath、rs_convexhull、rs_crs、
rs_envelope、rs_georeference、rs_numbands、rs_rastertoworldcoord、
rs_rastertoworldcoordx、rs_rastertoworldcoordy、rs_rotation、rs_setcrs、
rs_setsrid、rs_srid、rs_worldtorastercoord、rs_worldtorastercoordx、
rs_worldtorastercoordy、st_affine、st_asewkb、st_asgeojson、st_concavehull、
st_force2d、st_force3d、st_force3dm、st_force4d、st_geomfromewkb、
st_geomfromewkt、st_geomfromwkbunchecked、st_interiorringn、st_linemerge、
st_nrings、st_numinteriorrings、st_numpoints、st_rotate、st_rotatex、
st_rotatey 和 st_scale 的用户将很高兴地看到这些函数现已可在 SedonaDB
工作流中使用。这使 `ST_` 和 `RS_` 函数总数达到 143 个,并尽可能在
完整的几何类型矩阵以及 XY、XYZ、XYM、XYZM 维度下与 PostGIS 进行了
兼容性测试。

在新增这些函数的过程中,我们发现返回大几何的 GEOS 函数(例如带参数的
`ST_Buffer()` 或 `ST_Union()`)可以快得多。在 0.3.0 中,我们改进了
将大型 GEOS 几何追加到输出数组的执行流程,使部分基准测试的性能
提升了多达 70%!

感谢 [2010YOUY01](https://github.com/2010YOUY01)、
[Abeeujah](https://github.com/Abeeujah)、
[jesspav](https://github.com/jesspav)、
[Kontinuation](https://github.com/Kontinuation)、
[petern48](https://github.com/petern48)、
[prantogg](https://github.com/prantogg) 和
[yutannihilation](https://github.com/yutannihilation) 的贡献!

## R 端 GDAL/OGR 读取支持

SedonaDB R 版的早期测试者对其潜力表示兴奋,但指出要让它真正可用,
就需要在不经过 sf DataFrame 的情况下读取 Shapefile、FlatGeoBuf、
GeoPackage 等空间格式。在 SedonaDB 0.3.0 中,我们新增了 `sd_read_sf()`,
它利用 sf 包中实验性提供的 GDAL ArrowArrayStream 特性。

```r
library(sedonadb)

# Works with files
fgb <- system.file("files/natural-earth_cities.fgb", package = "sedonadb")
sd_read_sf(fgb)
```

    <sedonab_dataframe: NA x 2>
    ┌────────────┬─────────────────────────────────────────────┐
    │    name    ┆                 wkb_geometry                │
    │    utf8    ┆                   geometry                  │
    ╞════════════╪═════════════════════════════════════════════╡
    │ Wellington ┆ POINT(174.77720094690068 -41.2920679923151) │
    ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Auckland   ┆ POINT(174.763027 -36.8480549)               │
    ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Melbourne  ┆ POINT(144.9730704 -37.8180855)              │
    ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Canberra   ┆ POINT(149.1290262 -35.2830285)              │
    ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Sydney     ┆ POINT(151.2125477744749 -33.87137339218338) │
    ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Suva       ┆ POINT(178.4417073 -18.1330159)              │
    └────────────┴─────────────────────────────────────────────┘
    Preview of up to 6 row(s)

R 端的 GDAL/OGR 支持比 Python 端更有限(例如,SQL 过滤不会自动下推到
数据源,必须在读取调用中显式指定);不过,它有望减少一些摩擦,并为
更多用例解锁 R 支持!

## R DataFrame API

最后但同样重要的是,我们为 SedonaDB 加入了 [dplyr](https://dplyr.tidyverse.org)
后端的基础构件,包括基础的表达式翻译以及对 r-spatial 生态中
大多数类几何对象的支持。这个 API 处于实验阶段,欢迎反馈!

```r
library(sedonadb)

url <- "https://github.com/geoarrow/geoarrow-data/releases/download/v0.2.0/ns-water_water-point.parquet"
df <- sd_read_parquet(url)

df |>
  sd_group_by(FEAT_CODE) |>
  sd_summarise(
    n = n(),
    geometry = st_collect_agg(geometry)
  ) |>
  sd_arrange(desc(n))
```

    <sedonab_dataframe: NA x 3>
    ┌───────────┬───────┬──────────────────────────────────────────────────────────┐
    │ FEAT_CODE ┆   n   ┆                         geometry                         │
    │    utf8   ┆ int64 ┆                         geometry                         │
    ╞═══════════╪═══════╪══════════════════════════════════════════════════════════╡
    │ WARK60    ┆ 44406 ┆ MULTIPOINT Z((534300.0192 4986233.3817 82.3999999999941… │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ WARA60    ┆   193 ┆ MULTIPOINT Z((291828.02660000045 4851581.779999999 18.3… │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ WAFA60    ┆    90 ┆ MULTIPOINT Z((552953.3191 4974220.8818 0.95799999999871… │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ WADM60    ┆     1 ┆ MULTIPOINT Z((421205.36319999956 4983412.968 22.8999999… │
    └───────────┴───────┴──────────────────────────────────────────────────────────┘
    Preview of up to 6 row(s)

感谢 [e-kotov](https://github.com/e-kotov) 提出的详细需求和驱动用例!

## 下一步计划

在 0.3.0 开发周期中,我们合并了两项激动人心的贡献的初步成果:
基于 GPU 加速的空间连接,以及对 LAS 和 LAZ 点云格式的读取支持。
在 0.4.0 中,我们希望将这些组件打磨完善并正式上线!此外,我们还希望
扩展引擎对 Geography 数据类型的全面支持,并加速其当前实现。

## 贡献者

```shell
git shortlog -sn apache-sedona-db-0.3.0.dev..apache-sedona-db-0.3.0
    50  Dewey Dunnington
    45  Kristin Cowalcijk
    19  Hiroaki Yutani
     6  Balthasar Teuscher
     5  jp
     4  Peter Nguyen
     4  Yongting You
     3  Feng Zhang
     3  Liang Geng
     2  Abeeujah
     2  Matthew Powers
     1  Isaac Corley
     1  Jia Yu
     1  John Bampton
     1  Mayank Aggarwal
     1  Mehak3010
     1  Pranav Toggi
     1  eitsupi
```

[^1]: 不太常见,但 SedonaDB 也支持非地球 CRS。
