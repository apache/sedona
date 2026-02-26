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
title: "SedonaDB 0.3.0 Release"
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

# SedonaDB 0.3.0 Release

The Apache Sedona community is excited to announce the release of [SedonaDB](https://sedona.apache.org/sedonadb) version 0.3.0!

SedonaDB is the first open-source, single-node analytical database engine that treats spatial data as a first-class citizen. It is developed as a subproject of Apache Sedona. This release consists of [183 resolved issues](https://github.com/apache/sedona-db/milestone/2?closed=1) including 40 new functions from 18 contributors.

Apache Sedona powers large-scale geospatial processing on distributed engines like Spark (SedonaSpark), Flink (SedonaFlink), and Snowflake (SedonaSnow). SedonaDB extends the Sedona ecosystem with a single-node engine optimized for small-to-medium data analytics, delivering the simplicity and speed that distributed systems often cannot.

## Release Highlights

- Larger-than-memory spatial join
- Item/row-level CRS support
- Parameterized SQL queries
- Parquet reader and writer improvements
- GDAL/OGR write support
- Improved spatial function coverage and documentation
- R DataFrame API


```python
# pip install "apache-sedona[db]"
import sedona.db

sd = sedona.db.connect()
```

## Out of Core Spatial Join

The first release of SedonaDB almost four months ago included an extremely flexible and performant spatial join. As a refresher, a spatial join finds the interaction between two tables based on some spatial relationship (e.g., intersects or within distance). For example, if you're thinking of moving and you love pizza, SedonaDB can find you the list of addresses within 1km of a pizza restaurant in about XX seconds.


```python

```

The way that this join algorithm worked was approximately (1) read all the batches in one side of the spatial join into memory, (2) build a spatial index in memory, and (3) query it in parallel for every batch in the other side of the join, streaming the result. This is how most spatial data frame libraries do a join, although SedonaDB was able to leverage DataFusion's infrastructure for parallel streaming execution and Apache Arrow's compact representation of an array of geometries to efficiently perform most joins that the average user could think of.

Even though our initial join was fast, it had a hard limit: all of the uncompressed batches of the build side AND the spatial index had to fit in memory. For a middle-of-the-road laptop with 16 GB of memory, this is somewhere around 200 million points, not accounting for any space required for non-geometry columns: plenty for most analyses, but not for large datasets or resource-limited environments like containers or cloud deployments.

We also had a unique resource at our disposal: the Apache Sedona community! Sedona Spark has been able to perform distributed spatial joins for many years, and its authors (Jia Yu, Kristin Cowalcijk, and Feng Zhang) are all active SedonaDB contributors. After our 0.2 release we asked the question: can we use the same ideas behind Sedona Spark's spatial join to allow SedonaDB to join...*anything*?

The result is a mostly rewritten implementation of the spatial join (which includes the k-nearest neighbours or KNN join) that allows SedonaDB users to be truly fearless: if you can write the query, SedonaDB can finish the job. This is a complex enough feature that will be the subject of a dedicated future blog post, but as a quick example: SedonaDB can now find you the closest pizza restuarant to every address in the United States in XXX seconds.


```python

```

"Thank you" seems like a completely inadequate thing to say given the amount of work it took to make this happen, so we will just tell you that [@Kontinuation](https://github.com/Kontinuation) designed and implemented nearly all of this and let you be appropriately impressed.

## Item/Row-level CRS Support

Since our first release SedonaDB has supported coordinate reference systems (CRS) following the idiom of GeoParquet, GeoPandas, and other spatial data frame libraries: every column can optionally declare a CRS that defines how the coordinates of each value relate to the surface of the earth[^1]. This is efficient because it amortizes the cost of considering the CRS to once per query or once per batch; however, this pattern made it awkward to interact with systems like PostGIS and Sedona Spark that allow each item to declare its own CRS. Per-item CRSes have other uses, too, such as performing computations in meter-based local CRSes or reading in data from multiple sources and using SedonaDB to transform to a common CRS.

Item level CRS support was added to SedonaDB in version 0.2; however, in the 0.3 release we added support throughout the stack such that columns of this type work in all functions.

[^1]: It is significantly less common, but SedonaDB also supports non-Earth CRSes.


```python

```

## Parameterized queries

While the SedonaDB Python bindings expose a limited DataFrame API, to date the primary way to interact with SedonaDB in any meaningful way is SQL. SQL is well-supported by LLMs and allows us to leverage DataFusion's excellent SQL parser; however, it made interacting with SedonaDB in a programmatic environment awkward and completely reliant on serializing query parameters to SQL.

In SedonaDB 0.3.0, we added parameterized query support in R and Python.


```python

```

## Parquet Improvements

When SedonaDB was first launched, the Parquet Geometry and Geography types had [just been added to the Parquet specification](https://cloudnativegeo.org/blog/2025/02/geoparquet-2.0-going-native/) and support in the ecosystem was sparse. As of SedonaDB 0.3.0, Parquet files that define geometry and geography columns are supported out of the box! Functionally this means that SedonaDB no longer needs an extra (bbox) column and GeoParquet metadata to query a Parquet file efficiently: with the built-in geometry and geography types came embedded statistics to reduce file size for many applications.


```python
# A Parquet file with no GeoParquet metadata
url = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_countries.parquet"
sd.read_parquet(url).schema
```




    SedonaSchema with 3 fields:
      name: utf8<Utf8View>
      continent: utf8<Utf8View>
      geometry: geometry<WkbView(ogc:crs84)>



Even though SedonaDB can now read such files, it cannot yet write them (keep an eye out for this feature in 0.4.0!). For more on spatial types in Parquet, see [the recent deep dive on the Parquet blog](https://parquet.apache.org/blog/2026/02/13/native-geospatial-types-in-apache-parquet/).

## GDAL/OGR Write Support

In SedonaDB 0.2.0 we added the ability to read a wide variety of spatial file formats like Shapefile, FlatGeoBuf, and GeoPackage powered by the fantastic GDAL/OGR and the [pyogrio package](https://github.com/geopandas/pyogrio)'s Arrow interface. In 0.3.0 we added write support! Because both reads and writes are streaming, this makes SedonaDB an excellent choice for arbitrary read/write/convert workflows that scale to any size of input.


```python

```

## Improved spatial function coverage

Since the 0.2.0 release we have been fortunate to work with contributors add 36 new `ST_` and `RS_` functions to our growing catalogue. Users of rs_bandpath, rs_convexhull, rs_crs, rs_envelope, rs_georeference, rs_numbands, rs_rastertoworldcoord, rs_rastertoworldcoordx, rs_rastertoworldcoordy, rs_rotation, rs_setcrs, rs_setsrid, rs_srid, rs_worldtorastercoord, rs_worldtorastercoordx, rs_worldtorastercoordy, st_affine, st_asewkb, st_asgeojson, st_concavehull, st_force2d, st_force3d, st_force3dm, st_force4d, st_geomfromewkb, st_geomfromewkt, st_geomfromwkbunchecked, st_interiorringn, st_linemerge, st_nrings, st_numinteriorrings, st_numpoints, st_rotate, st_rotatex, st_rotatey, and st_scale will be pleased to know that these functions are now available in SedonaDB workflows.

In the process of adding some of these new functions, we discovered that GEOS-based functions that return large geometries (e.g., `ST_Buffer()` with parameters or `ST_Union()`) could be much faster. In 0.3.0 we improved the process of appending large GEOS geometries to our output arrays during execution which resulted in some benchmarks becoming up to 70% faster!

Thank you to [2010YOUY01](https://github.com/2010YOUY01), [Abeeujah](https://github.com/Abeeujah), [jesspav](https://github.com/jesspav), [Kontinuation](https://github.com/Kontinuation), [petern48](https://github.com/petern48), [prantogg](https://github.com/prantogg), and [yutannihilation](https://github.com/yutannihilation) for these contributions!

## R DataFrame API

Last but not least, we added the building blocks for a [dplyr](https://dplyr.tidyverse.org) backend to SedonaDB, including basic expression translation and support for most geometry-like objects in the r-spatial ecosystem. You can now

Thank you to [e-kotov](https://github.com/e-kotov) for the detailed feature request and motivating use case!


```python

```

## Contributors

```shell
git shortlog -sn apache-sedona-db-0.3.0.dev..HEAD
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
