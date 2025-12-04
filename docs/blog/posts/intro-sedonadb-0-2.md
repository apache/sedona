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
title: "SedonaDB 0.2.0 Release"
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

The Apache Sedona community is excited to announce the release of [SedonaDB](https://sedona.apache.org/sedonadb) version 0.2.0!

SedonaDB is the first open-source, single-node analytical database engine that treats spatial data as a first-class citizen. It is developed as a subproject of Apache Sedona. This release consists of [136 resolved issues](https://github.com/apache/sedona-db/milestone/1?closed=1) including 40 new functions from 17 contributors.

Apache Sedona powers large-scale geospatial processing on distributed engines like Spark (SedonaSpark), Flink (SedonaFlink), and Snowflake (SedonaSnow). SedonaDB extends the Sedona ecosystem with a single-node engine optimized for small-to-medium data analytics, delivering the simplicity and speed that distributed systems often cannot.

<!-- more -->

## Release Highlights

- Improved spatial function coverage
- GDAL/OGR spatial file format read support
- GeoParquet 1.1 write support
- Python user-defined function support
- Initial Raster data type implementation
- Release on `crates.io`
- Build system improvements

For a complete list of changes since SedonaDB 0.1.0 see the [milestone for 0.2.0](https://github.com/apache/sedona-db/milestone/1?closed=1).

## Improved spatial function coverage

Since the 0.1.0 release we have been fortunate to work with contributors add 40 new `ST_` and `RS_` functions to our growing catalogue. Users of rs_height, rs_scalex, rs_scaley, rs_skewx, rs_skewy, rs_upperleftx, rs_upperlefty, rs_width, st_azimuth, st_boundary, st_crosses, st_dump, st_endpoint, st_geometryfromtext, st_geometryn, st_isclosed, st_iscollection, st_isring, st_issimple, st_isvalid, st_isvalidreason, st_makevalid, st_minimumclearance, st_minimumclearanceline, st_npoints, st_numgeometries, st_overlaps, st_pointn, st_points, st_polygonize, st_polygonize_agg, st_reverse, st_simplify, st_simplifypreservetopology, st_snap, st_startpoint, st_translate, st_unaryunion, and st_zmflag will be pleased to know that these functions are now available in SedonaDB workflows.

Thank you to [Abeeujah](https://github.com/Abeeujah), [ayushjariyal](https://github.com/ayushjariyal), [jesspav](https://github.com/jesspav), [joonaspessi](https://github.com/joonaspessi), [petern48](https://github.com/petern48), and [yutannihilation](https://github.com/yutannihilation) for these contributions! (With a special thanks to [petern48](https://github.com/petern48) for reviewing nearly all of them!)

## GDAL/OGR spatial file format read support

Whereas SedonaDB 0.1.0 launched with GeoParquet read support and GeoPandas interoperatiblity, support for file formats like GeoPackage, Shapefile, FlatGeoBuf inherited the limitations of GeoPandas (notably, the materialization of an entire layer in memory as a Pandas dataframe). The package powering GeoPandas read support ([pyogrio](https://github.com/geopandas/pyogrio)) also exposes the [underlying provider's (GDAL/OGR) native Arrow interface](https://gdal.org/en/stable/development/rfc/rfc86_column_oriented_api.html), which is the exact format that SedonaDB uses under the hood! This allowed us to add support for dozens of vector formats at once wired directly in to DataFusion's flexible `FileFormat` API. Users can now read from spatial file formats just as they can from Parquet:

```python
# pip install "apache-sedona[db]"
import sedona.db

sd = sedona.db.connect()
url = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/example-crs/files/example-crs_vermont-utm.fgb"
sd.read_pyogrio(url).to_pandas().plot()
```

This works for local files, `https://` urls (via `/vsicurl/`), and zipped files (via `/vsizip/`). Globs (i.e., `sd.read_pyogrio("path/to/*.gpkg")`) resolving to 1 or more local files are also supported.

Like SedonaDB's GeoParquet support, spatial filters like `ST_Intersects()` are translated into the GDAL/OGR scan where possible to take advantage of embedded spatial indexes in formats like GeoPackage, FlatGeoBuf, and Shapefile. For example, we can query small areas of a [huge FlatGeoBuf file hosted elsewhere](https://flatgeobuf.org/examples/maplibre/large.html) without scanning the entire file:

```python
# 12 GB file
url = "https://flatgeobuf.septima.dk/population_areas.fgb"
sd.read_pyogrio(url).to_view("population_areas")

wkt = "POLYGON ((-73.978329 40.767412, -73.950005 40.767412, -73.950005 40.795098, -73.978329 40.795098, -73.978329 40.767412))"
sd.sql(
    f"""
SELECT sum(population::INTEGER) FROM population_areas
WHERE ST_Intersects(wkb_geometry, ST_SetSRID(ST_GeomFromWKT('{wkt}'), 4326))
"""
).show()
# > ┌──────────────────────────────────┐
# > │ sum(population_areas.population) │
# > │               int64              │
# > ╞══════════════════════════════════╡
# > │                           256251 │
# > └──────────────────────────────────┘
```

## GeoParquet 1.1 Write Support

Whereas the initial version of SedonaDB launched with basic write support for GeoParquet files, the latest version of the specification that enables readers to read small portions of the resulting Parquet file was not supported. With the latest release, `DataFrame.to_parquet("path/to/parquet", geoparquet_version="1.1")` will add a `bbox` column enabling functions like `sd.read_parquet()` with a `WHERE ST_Intersects()` query to read only a portion of the input file.

```python
# pip install "apache-sedona[db]"
import sedona.db

sd = sedona.db.connect()
url = "https://github.com/geoarrow/geoarrow-data/releases/download/v0.2.0/ns-water_water-point.fgb"

sd.read_pyogrio(url).to_parquet("water_point.parquet", geoparquet_version="1.1")
```

Shrinking the default row group size to ~100,000 and applying a spatial sort may improve read performance by ensuring that row groups contain related features.

```python
sd.sql("SET datafusion.execution.parquet.max_row_group_size = 100000")

sd.read_parquet(url).to_view("water_point")

sd.sql(
    """
SELECT * FROM water_point
ORDER BY sd_order(geometry)
"""
).to_parquet("water_point.parquet", geoparquet_version="1.1")
```

## Python User-Defined Function Support

User-defined functions (UDFs) are essential components of many workflows in modern DataFrame engines like Spark, DataFusion, and DuckDB to capture user-specific logic that is difficult or impossible to implement by simply composing existing functions. Just as [SedonaSpark provides a vectorized UDF framework for geometry types](https://sedona.apache.org/latest/tutorial/sql/#spatial-vectorized-udfs-python-only), SedonaDB 0.2.0 exposes a framework that allows user-specific logic (including but not limited to those involving geometry!) to be referenced from SQL workflows. For example, a UDF implementation of `ST_Buffer()` could be written as:

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

See the [documentation page for `arrow_udf()`](https://sedona.apache.org/sedonadb/latest/reference/python/#sedonadb.udf.arrow_udf) for more examples and documentation.

## Initial Raster data type implementation

The [raster data type support in Sedona Spark](https://sedona.apache.org/latest/tutorial/raster) is a popular feature and in SedonaDB 0.2.0 we are pleased to offer a raster data type and a few basic functions!

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

For more information or to get involved see the [raster support umbrella issue](https://github.com/apache/sedona-db/issues/246). Thank you to [jesspav](https://github.com/jesspav) for driving this work!

## Release on `crates.io`

Because our initial development of SedonaDB was closely tied to improvements we were experimenting with in some of our dependency crates from the GeoRust ecosystem, our first release of SedonaDB contained git dependencies and references to the forks we had used in our experiments. While the first release *could* be used in Rust projects via a git dependency, this prevented any downstream project from releasing to crates.io themselves and did not make clear that we do expose a public Rust API that can be used alongside any DataFusion-based project! Rust projects can use the components we provide or use the pre-assembled `SedonaContext`.

SedonaDB 0.2.0, in addition to being [released to crates.io](https://crates.io/crates/sedona) includes [a Rust example](https://github.com/apache/sedona-db/tree/main/examples/sedonadb-rust) to get interested Rust projects started:

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

We're still learning about how or if downstream projects are interested in using components from Rust, so feel free to [open an issue](https://github.com/apache/sedona-db/issues/new) with ideas or questions.

Special thanks to [kylebarron](https://github.com/kylebarron) for reviewing our PRs upstream into the [geo-index](https://github.com/kylebarron/geo-index) and [wkb](https://github.com/georust/wkb) crates!

## Build System Improvements

Build system improvements are some of the least glorious but most important tasks included in the SedonaDB 0.2.0 release; however, we are pleased to be able to release SedonaDB 0.2.0 Python binaries with support for PROJ, GEOS, and S2Geography on MacOS, Windows, and Linux (Python 3.9 to 3.14, including free-threaded variants of Python 3.13 and 3.14). [SedonaDB 0.2.0 for R may be installed via R-Universe](https://apache.r-universe.dev/sedonadb), which now includes pre-built binaries for MacOS and Windows users. Support for PROJ on Windows and S2Geography on all R platforms is a work in progress and will hopefully be included in the next release.

Special thanks to [yutannihilation](https://github.com/yutannihilation) for contributing fixes to the R build system!

## Contributors

This release consists of contributions from 17 contributors. Thank you for your contributions!

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
