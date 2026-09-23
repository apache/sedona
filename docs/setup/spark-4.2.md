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

# Spark 4.2 native spatial types

Sedona 2.0 uses Spark's native `GEOMETRY` and `GEOGRAPHY` SQL types on Spark 4.2. Use the `sedona-spark-shaded-4.2_2.13` artifact and Java 17 or later. Sedona builds for earlier Spark versions continue to use Sedona's geometry and geography UDTs.

Sedona constructors and geometry-producing functions return native spatial columns. Native Spark functions and Sedona functions can consume the same columns:

```sql
SELECT ST_SRID(ST_Buffer(ST_GeomFromWKB(unhex(
  '0101000000000000000000F03F0000000000000040'), 3857), 1.0));
-- 3857
```

## Functions provided by Spark

On Spark 4.2, Sedona leaves these five names registered to Spark, including when Sedona is registered repeatedly or unregistered. Sedona does not provide aliases for its older implementations. Existing Python and Scala wrappers resolve to Spark's implementation.

| Function | Spark 4.2 signature and behavior |
| --- | --- |
| `ST_AsBinary` | Geometry or geography, with an optional byte-order argument. Produces WKB without an embedded SRID. |
| `ST_GeomFromWKB` | Binary WKB, with an optional SRID; the default is 0. |
| `ST_GeogFromWKB` | Binary WKB; the SRID is 4326. Sedona's wrapper with an explicit SRID applies `ST_SetSRID` to this result. |
| `ST_SRID` | Reads the SRID of either native spatial type. |
| `ST_SetSRID` | Sets the SRID metadata; it does not transform coordinates. |

Convert hexadecimal strings with `unhex` before passing them to Spark's WKB constructors. Use Sedona's `ST_GeomFromEWKB` or `ST_GeomFromEWKT` when an input embeds its SRID, and `ST_AsEWKB` or `ST_AsEWKT` when an output must include it. Spark's WKB constructors do not replace these EWKB/EWKT functions.

Native types validate SRIDs against Spark's supported coordinate reference systems. Geometry can use SRID 0. Geography requires a geographic CRS, and Sedona's text geography constructors now default to 4326 on Spark 4.2. Assign a valid geographic SRID before converting an SRID-0 geometry to geography.

See Spark's [spatial type reference](https://spark.apache.org/docs/4.2.0/sql-ref-geospatial-types.html) and [built-in function reference](https://spark.apache.org/docs/4.2.0/sql-ref-functions-builtin.html#geospatial-functions) for native function semantics.

## Schemas and persisted data

A native type can have a fixed SRID, such as `GEOMETRY(4326)`, or allow different SRIDs in different rows with `GEOMETRY(ANY)`. Sedona uses a fixed SRID when a constructor or operation can determine it; other results use `ANY`.

Spark's native Parquet writer requires a fixed SRID, and Spark 4.2 does not allow narrowing `ANY` to a fixed SRID with `CAST`. When the coordinates use a known CRS, assign that SRID with a literal argument to `ST_SetSRID` before writing:

```sql
SELECT ST_SetSRID(ST_Collect_Agg(geom), 4326) AS geom
FROM points;
```

`ST_SetSRID` assigns metadata without validating the previous SRIDs or transforming coordinates. Verify the input CRS first; use `ST_Transform` when coordinates need reprojection. Sedona's GeoParquet format also supports native geometry columns and retains its existing metadata-based CRS handling.

Existing files containing Sedona UDT columns remain readable by Sedona. Convert a legacy geometry column into a native column explicitly before using Spark's five built-ins:

```sql
SELECT ST_GeomFromEWKB(ST_AsEWKB(legacy_geom)) AS geom
FROM legacy_table;
```

Do not reinterpret Sedona's private UDT bytes as WKB. The conversion above preserves the geometry's SRID through EWKB.

## Scala, Java, and Python values

Collecting a native geometry column returns Spark's public spatial value instead of a JTS or Shapely object. In Scala and Java, use `org.apache.spark.sql.types.Geometry` and `Geography`. Sedona's RDD adapters convert between native DataFrame values and JTS spatial RDD values.

In Python, `sedona.spark.sql.types.GeometryType` and `GeographyType` refer to the native PySpark types. Specify an SRID, or use `geometry_type()` and `geography_type()` for schemas that allow any SRID. Use `to_spark_geometry`, `to_spark_geography`, and `to_shapely` for explicit Shapely conversion; see the [Python installation guide](install-python.md#spatial-values-on-spark-42-and-later).

Sedona's GeoPandas and GeoArrow interfaces handle these conversions at their API boundaries. `sedona_vectorized_udf` continues to accept Shapely/GeoSeries callbacks; on Spark 4.2 it uses Spark's standard pandas UDF protocol with native spatial inputs and outputs.

## Spark 4.2 limitations

Spark 4.2.0's default columnar cache does not support native spatial columns. A DataFrame `cache()` or `persist()` that uses this cache can fail with an unsupported geometry/geography type. Keep spatial values in WKB columns while caching and reconstruct native values afterward, carrying SRIDs separately where needed. Sedona's DBSCAN implementation keeps only vertex identifiers in its internal graph cache and joins cluster labels back to the original rows.

Spark 4.2 also requires every member of a native collection to use the same coordinate layout (XY, XYZ, XYM, or XYZM). A mixed-dimensional collection accepted by legacy Sedona is rejected at the native boundary. Normalize member dimensions, for example with `ST_Force2D` when discarding Z/M is intended, before creating the native collection.
