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

The Apache Sedona community is excited to announce the release of
[SedonaDB](https://sedona.apache.org/sedonadb) version 0.3.0!

SedonaDB is the first open-source, single-node analytical database
engine that treats spatial data as a first-class citizen. It is
developed as a subproject of Apache Sedona. This release consists of
[187 resolved
issues](https://github.com/apache/sedona-db/milestone/2?closed=1)
including 36 new functions from 18 contributors.

Apache Sedona powers large-scale geospatial processing on distributed
engines like Spark (SedonaSpark), Flink (SedonaFlink), and Snowflake
(SedonaSnow). SedonaDB extends the Sedona ecosystem with a single-node
engine optimized for small-to-medium data analytics, delivering the
simplicity and speed that distributed systems often cannot.

<!-- more -->

## Release Highlights

We’re excited to have so many things to highlight in this release!

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
sd.options.interactive = True
```

## Out of Core Spatial Join

The first release of SedonaDB almost four months ago included an
extremely flexible and performant spatial join. As a refresher, a
spatial join finds the interaction between two tables based on some
spatial relationship (e.g., intersects or within distance). For example,
if you’re thinking of moving and you love pizza, SedonaDB can find you
the neighborhood with the most pizza restaurants about as fast as your
internet connection/hard drive can load [Overture Maps
Divisions](https://docs.overturemaps.org/guides/divisions/) and
[Places](https://docs.overturemaps.org/guides/places/) data.

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

The way that this join algorithm worked was approximately (1) read all
the batches in one side of the spatial join into memory, (2) build a
spatial index in memory, and (3) query it in parallel for every batch in
the other side of the join, streaming the result. This is how most
spatial data frame libraries do a join, although SedonaDB was able to
leverage DataFusion’s infrastructure for parallel streaming execution
and Apache Arrow’s compact representation of an array of geometries to
efficiently perform most joins that the average user could think of.

Even though our initial join was fast, it had a hard limit: all of the
uncompressed batches of the build side AND the spatial index had to fit
in memory. For a middle-of-the-road laptop with 16 GB of memory, this is
somewhere around 200 million points, not accounting for any space
required for non-geometry columns: plenty for most analyses, but not for
large datasets or resource-limited environments like containers or cloud
deployments.

We also had a unique resource at our disposal: the Apache Sedona
community! Sedona Spark has been able to perform distributed spatial
joins for many years, and its authors (Jia Yu, Kristin Cowalcijk, and
Feng Zhang) are all active SedonaDB contributors. After our 0.2 release
we asked the question: can we use the same ideas behind Sedona Spark’s
spatial join to allow SedonaDB to join…*anything*?

The result is a mostly rewritten implementation of the spatial join
(which includes the k-nearest neighbours or KNN join) that allows
SedonaDB users to be truly fearless: if you can write the query,
SedonaDB can finish the job. This is a complex enough feature that will
be the subject of a dedicated future blog post, but as a quick example:
SedonaDB can identify duplicate (or nearly duplicate) points in a 130
million point dataset in about 30 seconds with only 3 GB of memory!

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

For some perspective, DuckDB 1.5.0 (beta) needs about 12 GB of memory to
make this work and GeoPandas needs at least 28 GB.

“Thank you” seems like a completely inadequate thing to say given the
amount of work it took to make this happen, so we will just tell you
that [@Kontinuation](https://github.com/Kontinuation) designed and
implemented nearly all of this and hope that you are appropriately
impressed.

## Item/Row-level CRS Support

Since our first release SedonaDB has supported coordinate reference
systems (CRS) following the idiom of GeoParquet, GeoPandas, and other
spatial data frame libraries: every column can optionally declare a CRS
that defines how the coordinates of each value relate to the surface of
the earth[^1]. This is efficient because it amortizes the cost of
considering the CRS to once per query or once per batch; however, this
pattern made it awkward to interact with systems like PostGIS and Sedona
Spark that allow each item to declare its own CRS. Per-item CRSes have
other uses, too, such as performing computations in meter-based local
CRSes or reading in data from multiple sources and using SedonaDB to
transform to a common CRS.

Item level CRS support was added to SedonaDB in version 0.2; however, in
the 0.3 release we added support throughout the stack such that columns
of this type work in all functions. For example, if you want to
transform some input into a local CRS, SedonaDB can get you there:

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

Crucially, it can also get you back!

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

The main route to creating item-level CRSes is `ST_Transform()` and
`ST_SetSRID()`, as well as the last argument of `ST_Point()`,
`ST_GeomFromWKT()`, `ST_GeomFromWKB()`. The new functions
`ST_GeomFromEWKT()` and `ST_GeomFromEWKB()` always return item-level
CRSes (and SRIDs are preserved when exporting via `ST_AsEWKT()` and
`ST_AsEWKB()`). Item-level CRSes should be preserved across calls to all
other functions (just like type-level CRSes are in previous versions).
We’d love to hear more use cases to ensure our support covers the full
range of use cases in the wild!

## Parameterized queries

While the SedonaDB Python bindings expose a limited DataFrame API, to
date the primary way to interact with SedonaDB in any meaningful way is
SQL. SQL is well-supported by LLMs and allows us to leverage
DataFusion’s excellent SQL parser; however, it made interacting with
SedonaDB in a programmatic environment awkward and completely reliant on
serializing query parameters to SQL.

In SedonaDB 0.3.0, we added parameterized query support in R and Python
along with converters for most geometry-like and/or CRS-like objects.
For example, if you have some contextual information as a GeoPandas
DataFrame, you can now insert that into any SQL query without worrying
about setting the CRS or serializing to SQL.

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

## Parquet Improvements

When SedonaDB was first launched, the Parquet Geometry and Geography
types had [just been added to the Parquet
specification](https://cloudnativegeo.org/blog/2025/02/geoparquet-2.0-going-native/)
and support in the ecosystem was sparse. As of SedonaDB 0.3.0, Parquet
files that define geometry and geography columns are supported out of
the box! Functionally this means that SedonaDB no longer needs an extra
(bbox) column and GeoParquet metadata to query a Parquet file
efficiently: with the built-in geometry and geography types came
embedded statistics to reduce file size for many applications.

```python
# A Parquet file with no GeoParquet metadata
url = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_countries.parquet"
sd.read_parquet(url).schema
```

    SedonaSchema with 3 fields:
      name: utf8<Utf8View>
      continent: utf8<Utf8View>
      geometry: geometry<WkbView(ogc:crs84)>

Even though SedonaDB can now read such files, it cannot yet write them
(keep an eye out for this feature in 0.4.0!). For more on spatial types
in Parquet, see [the recent deep dive on the Parquet
blog](https://parquet.apache.org/blog/2026/02/13/native-geospatial-types-in-apache-parquet/).

In addition to read improvements, we also improved the interface for
writing Parquet files. In particular, writing friendly GeoParquet 1.1
files that work nicely with SedonaDB, GeoPandas, and other engine’s
spatial filter pushdown. This works nicely with SedonaDB’s GDAL/OGR read
support to produce optimized GeoParquet files from just about anything:

```python
url = "https://github.com/geoarrow/geoarrow-data/releases/download/v0.2.0/ns-water_water-point.fgb"
sd.read_pyogrio(url).to_parquet(
    "optimized.parquet",
    geoparquet_version="1.1",
    max_row_group_size=100_000,
    sort_by="wkb_geometry",
)
```

## GDAL/OGR Write Support

In SedonaDB 0.2.0 we added the ability to read a wide variety of spatial
file formats like Shapefile, FlatGeoBuf, and GeoPackage powered by the
fantastic GDAL/OGR and the [pyogrio
package](https://github.com/geopandas/pyogrio)’s Arrow interface. In
0.3.0 we added write support! Because both reads and writes are
streaming, this makes SedonaDB an excellent choice for arbitrary
read/write/convert workflows that scale to any size of input.

```python
sd.read_parquet("optimized.parquet").to_pyogrio("water-point.fgb")
```

## Improved spatial function coverage

Since the 0.2.0 release we have been fortunate to work with contributors to
add 36 new `ST_` and `RS_` functions to our growing catalogue. Users of
rs_bandpath, rs_convexhull, rs_crs, rs_envelope, rs_georeference,
rs_numbands, rs_rastertoworldcoord, rs_rastertoworldcoordx,
rs_rastertoworldcoordy, rs_rotation, rs_setcrs, rs_setsrid, rs_srid,
rs_worldtorastercoord, rs_worldtorastercoordx, rs_worldtorastercoordy,
st_affine, st_asewkb, st_asgeojson, st_concavehull, st_force2d,
st_force3d, st_force3dm, st_force4d, st_geomfromewkb, st_geomfromewkt,
st_geomfromwkbunchecked, st_interiorringn, st_linemerge, st_nrings,
st_numinteriorrings, st_numpoints, st_rotate, st_rotatex, st_rotatey,
and st_scale will be pleased to know that these functions are now
available in SedonaDB workflows. This brings the total `ST_` and `RS_`
function count to 143, all of which are tested against PostGIS for
compatibility with the full matrix of geometry types and XY, XYZ, XYM,
and XYZM wherever possible.

In the process of adding some of these new functions, we discovered that
GEOS-based functions that return large geometries (e.g., `ST_Buffer()`
with parameters or `ST_Union()`) could be much faster. In 0.3.0 we
improved the process of appending large GEOS geometries to our output
arrays during execution which resulted in some benchmarks becoming up to
70% faster!

Thank you to [2010YOUY01](https://github.com/2010YOUY01),
[Abeeujah](https://github.com/Abeeujah),
[jesspav](https://github.com/jesspav),
[Kontinuation](https://github.com/Kontinuation),
[petern48](https://github.com/petern48),
[prantogg](https://github.com/prantogg), and
[yutannihilation](https://github.com/yutannihilation) for these
contributions!

## R GDAL/OGR read support

Early testers of SedonaDB for R expressed excitement over the potential
but noted that to be useful it would have to provide a way to read spatial
formats like Shapefile, FlatGeoBuf, and GeoPackage without going through
an sf DataFrame first. In SedonaDB 0.3.0, we added `sd_read_sf()`, which
leverages the GDAL ArrowArrayStream feature that is experimentally
available from the sf package.

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

R GDAL/OGR support is more limited than the same feature in Python (e.g.,
SQL filters are not automatically pushed down into the data source
and instead must be specified in the read call); however, it hopefully
removes some friction and unlocks R support for a wider variety of
use cases!

## R DataFrame API

Last but not least, we added the building blocks for a
[dplyr](https://dplyr.tidyverse.org) backend to SedonaDB, including
basic expression translation and support for most geometry-like objects
in the r-spatial ecosystem. This API is experimental and feedback is
welcome!

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

Thank you to [e-kotov](https://github.com/e-kotov) for the detailed
feature request and motivating use case!

## What’s Next?

During the 0.3.0 development cycle we merged the first steps of two
exciting contributions: A GPU-accelerated spatial join and support for
reading point cloud formats LAS and LAZ. In 0.4.0 we hope to have these
components polished and ready to go! Finally, we hope to broaden our
support for the Geography data type throughout the engine and
accelerate its current implementation.

## Contributors

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

[^1]: It is significantly less common, but SedonaDB also supports
    non-Earth CRSes.
