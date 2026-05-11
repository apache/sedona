---
date:
  created: 2025-12-01
links:
  - SedonaDB: https://sedona.apache.org/sedonadb/
authors:
  - dewey
  - kristin
  - feng
  - peter
  - jess
  - jia
  - matt_powers
title: "SedonaDB 0.2.0 版本发布"
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

Apache Sedona 社区激动地宣布 [SedonaDB](https://sedona.apache.org/sedonadb) 0.2.0 版本正式发布!

SedonaDB 是首款将空间数据视为一等公民的开源单节点分析数据库引擎,作为 Apache Sedona 的子项目开发。本次版本包含 [136 个已解决的 issue](https://github.com/apache/sedona-db/milestone/1?closed=1),来自 17 位贡献者,并新增 40 个函数。

Apache Sedona 在 Spark(SedonaSpark)、Flink(SedonaFlink)和 Snowflake(SedonaSnow)等分布式引擎上提供大规模地理空间处理能力。SedonaDB 将 Sedona 生态系统扩展到单节点引擎,专为中小规模数据分析进行优化,提供分布式系统通常难以企及的简洁性与速度。

<!-- more -->

## 版本亮点

- 空间函数覆盖度提升
- 支持 GDAL/OGR 空间文件格式读取
- 支持 GeoParquet 1.1 写入
- 支持 Python 用户自定义函数
- 初步实现栅格数据类型
- 发布到 `crates.io`
- 构建系统改进

完整的 0.2.0 相对 0.1.0 的变更列表请参见 [0.2.0 里程碑](https://github.com/apache/sedona-db/milestone/1?closed=1)。

## 空间函数覆盖度提升

自 0.1.0 发布以来,我们很荣幸与众多贡献者合作,为不断壮大的函数库新增了 40 个 `ST_` 和 `RS_` 函数。rs_height、rs_scalex、rs_scaley、rs_skewx、rs_skewy、rs_upperleftx、rs_upperlefty、rs_width、st_azimuth、st_boundary、st_crosses、st_dump、st_endpoint、st_geometryfromtext、st_geometryn、st_isclosed、st_iscollection、st_isring、st_issimple、st_isvalid、st_isvalidreason、st_makevalid、st_minimumclearance、st_minimumclearanceline、st_npoints、st_numgeometries、st_overlaps、st_pointn、st_points、st_polygonize、st_polygonize_agg、st_reverse、st_simplify、st_simplifypreservetopology、st_snap、st_startpoint、st_translate、st_unaryunion 和 st_zmflag 的用户将很高兴地看到这些函数现已可在 SedonaDB 工作流中使用。

感谢 [Abeeujah](https://github.com/Abeeujah)、[ayushjariyal](https://github.com/ayushjariyal)、[jesspav](https://github.com/jesspav)、[joonaspessi](https://github.com/joonaspessi)、[petern48](https://github.com/petern48) 和 [yutannihilation](https://github.com/yutannihilation) 的贡献!(特别感谢 [petern48](https://github.com/petern48),他几乎评审了所有这些函数!)

## GDAL/OGR 空间文件格式读取支持

SedonaDB 0.1.0 发布时支持 GeoParquet 读取和与 GeoPandas 的互操作,但对 GeoPackage、Shapefile、FlatGeoBuf 等文件格式的支持继承自 GeoPandas 的局限(尤其是将整个图层在内存中物化为 Pandas DataFrame)。驱动 GeoPandas 读取支持的库 ([pyogrio](https://github.com/geopandas/pyogrio)) 同时也暴露了[底层提供方(GDAL/OGR)的原生 Arrow 接口](https://gdal.org/en/stable/development/rfc/rfc86_column_oriented_api.html),而这恰好正是 SedonaDB 在底层使用的格式!这让我们能够一次性接入大量矢量格式,并直接连入 DataFusion 灵活的 `FileFormat` API。现在用户可以像读取 Parquet 一样读取空间文件格式:

```python
# pip install "apache-sedona[db]"
import sedona.db

sd = sedona.db.connect()
url = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/example-crs/files/example-crs_vermont-utm.fgb"
sd.read_pyogrio(url).to_pandas().plot()
```

它支持本地文件、`https://` URL(通过 `/vsicurl/`)以及压缩文件(通过 `/vsizip/`)。匹配到 1 个或多个本地文件的 glob 模式(例如 `sd.read_pyogrio("path/to/*.gpkg")`)也同样受支持。

和 SedonaDB 的 GeoParquet 支持类似,`ST_Intersects()` 等空间过滤器会尽可能下推到 GDAL/OGR 扫描层,以利用 GeoPackage、FlatGeoBuf 和 Shapefile 等格式中嵌入的空间索引。例如,我们可以从[远端托管的大型 FlatGeoBuf 文件](https://flatgeobuf.org/examples/maplibre/large.html)中查询小区域,而无需扫描整个文件:

```python
# 12 GB file
url = "https://flatgeobuf.septima.dk/population_areas.fgb"
sd.read_pyogrio(url).to_view("population_areas")

wkt = "POLYGON ((-73.978329 40.767412, -73.950005 40.767412, -73.950005 40.795098, -73.978329 40.795098, -73.978329 40.767412))"
sd.sql(f"""
SELECT sum(population::INTEGER) FROM population_areas
WHERE ST_Intersects(wkb_geometry, ST_SetSRID(ST_GeomFromWKT('{wkt}'), 4326))
""").show()
# > ┌──────────────────────────────────┐
# > │ sum(population_areas.population) │
# > │               int64              │
# > ╞══════════════════════════════════╡
# > │                           256251 │
# > └──────────────────────────────────┘
```

## GeoParquet 1.1 写入支持

SedonaDB 的首个版本提供了 GeoParquet 文件的基础写入能力,但当时尚不支持规范的最新版本——也就是允许读取器仅读取生成 Parquet 文件中一小部分内容的能力。在最新版本中,`DataFrame.to_parquet("path/to/parquet", geoparquet_version="1.1")` 会添加 `bbox` 列,使 `sd.read_parquet()` 配合 `WHERE ST_Intersects()` 查询时只读取输入文件的一部分。

```python
# pip install "apache-sedona[db]"
import sedona.db

sd = sedona.db.connect()
url = "https://github.com/geoarrow/geoarrow-data/releases/download/v0.2.0/ns-water_water-point.fgb"

sd.read_pyogrio(url).to_parquet("water_point.parquet", geoparquet_version="1.1")
```

将默认行组大小缩小到 ~100,000 并进行空间排序可能会提升读取性能——这能确保行组内包含相关性较高的要素。

```python
sd.sql("SET datafusion.execution.parquet.max_row_group_size = 100000")

sd.read_parquet(url).to_view("water_point")

sd.sql("""
SELECT * FROM water_point
ORDER BY sd_order(geometry)
""").to_parquet("water_point.parquet", geoparquet_version="1.1")
```

## Python 用户自定义函数支持

用户自定义函数(UDF)是 Spark、DataFusion、DuckDB 等现代 DataFrame 引擎中众多工作流的关键组件,用于承载难以通过组合现有函数实现的用户特定逻辑。和 [SedonaSpark 为几何类型提供向量化 UDF 框架](https://sedona.apache.org/latest/tutorial/sql/#spatial-vectorized-udfs-python-only)类似,SedonaDB 0.2.0 提供了一个框架,允许从 SQL 工作流中引用用户特定逻辑(包括但不限于涉及几何的!)。例如,可以这样实现 `ST_Buffer()` 的 UDF:

```python
import pyarrow as pa
import sedona.db
from sedonadb import udf
import shapely
import geoarrow.pyarrow as ga

sd = sedona.db.connect()


@udf.arrow_udf(ga.wkb(), [udf.GEOMETRY, udf.NUMERIC])
def shapely_udf(geom, distance):
    geom_wkb = pa.array(geom.storage.to_array())
    distance = pa.array(distance.to_array())
    geom = shapely.from_wkb(geom_wkb)
    result_shapely = shapely.buffer(geom, distance)
    return pa.array(shapely.to_wkb(result_shapely))


sd.register_udf(shapely_udf)
sd.sql("SELECT shapely_udf(ST_Point(0, 0), 2.0) as col").show()
# > ┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
# > │                                                col                                               │
# > │                                             geometry                                             │
# > ╞══════════════════════════════════════════════════════════════════════════════════════════════════╡
# > │ POLYGON((2 0,1.9615705608064609 -0.3901806440322565,1.8477590650225735 -0.7653668647301796,1.66… │
# > └──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

更多示例和文档请参见 [`arrow_udf()` 的文档页](https://sedona.apache.org/sedonadb/latest/reference/python/#sedonadb.udf.arrow_udf)。

## 初步的栅格数据类型实现

[Sedona Spark 中的栅格数据类型支持](https://sedona.apache.org/latest/tutorial/raster) 是一项广受欢迎的特性,在 SedonaDB 0.2.0 中我们也很高兴地提供了栅格数据类型以及一些基础函数!

```python
import sedona.db

sd = sedona.db.connect()

sd.sql("SELECT RS_Width(RS_Example()) as width").show()
# > ┌────────┐
# > │  width │
# > │ uint64 │
# > ╞════════╡
# > │     64 │
# > └────────┘
```

更多信息或参与方式请参见[栅格支持的总览 issue](https://github.com/apache/sedona-db/issues/246)。感谢 [jesspav](https://github.com/jesspav) 推动这项工作!

## 发布到 `crates.io`

由于 SedonaDB 的早期开发与我们在 GeoRust 生态中所依赖的一些 crate 改进紧密相关,首个 SedonaDB 版本中含有 git 依赖和指向我们实验所用 fork 的引用。虽然首个版本*能够*通过 git 依赖在 Rust 项目中使用,但这阻碍了下游项目发布到 crates.io,也无法清晰地表明我们确实公开了可以与任何基于 DataFusion 的项目一起使用的 Rust 公共 API!Rust 项目可以使用我们提供的组件,也可以使用预先组装好的 `SedonaContext`。

SedonaDB 0.2.0 不仅[发布到了 crates.io](https://crates.io/crates/sedona),还附带[一个 Rust 示例](https://github.com/apache/sedona-db/tree/main/examples/sedonadb-rust),帮助有兴趣的 Rust 项目快速上手:

```toml
[package]
# ...

[dependencies]
datafusion = { version = "50.2.0"}
sedona = { version = "0.2.0" }
# ...
```

```rust
use datafusion::{common::Result, prelude::*};
use sedona::context::{SedonaContext, SedonaDataFrame};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SedonaContext::new_local_interactive().await?;
    let url = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities_geo.parquet";
    let df = ctx.read_parquet(url, Default::default()).await?;
    let output = df
        .sort_by(vec![col("name")])?
        .show_sedona(&ctx, Some(5), Default::default())
        .await?;
    println!("{output}");
    Ok(())
}
```

我们仍在了解下游项目是否对使用 Rust 组件感兴趣,所以欢迎你[创建 issue](https://github.com/apache/sedona-db/issues/new) 提出想法或问题。

特别感谢 [kylebarron](https://github.com/kylebarron) 在我们向上游 [geo-index](https://github.com/kylebarron/geo-index) 和 [wkb](https://github.com/georust/wkb) crate 提交 PR 时进行的评审!

## 构建系统改进

构建系统的改进是 SedonaDB 0.2.0 版本中最不显眼但又最重要的工作之一;不过,我们很高兴能够发布支持 PROJ、GEOS 和 S2Geography 的 SedonaDB 0.2.0 Python 二进制(覆盖 MacOS、Windows 和 Linux,Python 3.9 到 3.14,包括 Python 3.13 和 3.14 的 free-threaded 变体)。[SedonaDB 0.2.0 的 R 版本可通过 R-Universe 安装](https://apache.r-universe.dev/sedonadb),现在 MacOS 和 Windows 用户都有预编译二进制可用。Windows 上的 PROJ 支持以及所有 R 平台上的 S2Geography 支持仍在推进中,有望在下一个版本中加入。

特别感谢 [yutannihilation](https://github.com/yutannihilation) 为 R 构建系统贡献了修复!

## 贡献者

本次版本来自 17 位贡献者的工作。感谢你们的贡献!

```
git shortlog -sn apache-sedona-db-0.2.0.dev..apache-sedona-db-0.2.0
    25  Dewey Dunnington
    16  Abeeujah
    13  Hiroaki Yutani
    13  Peter Nguyen
    12  Kristin Cowalcijk
     8  jp
     6  Feng Zhang
     4  Joonas Pessi
     3  Matthew Powers
     2  Cancai Cai
     2  Jia Yu
     1  L_Sowmya
     1  Liang Geng
     1  Peter Von der Porten
     1  Yongting You
     1  ayushjariyal
     1  dentiny
```
