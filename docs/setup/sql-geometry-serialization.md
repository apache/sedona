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

# Spark SQL spatial types and serialization

Starting in v2.0.0, Sedona's `GeometryUDT` uses standard EWKB internally. It remains the default SQL geometry type on Spark 3.5–4.1 and is available explicitly on Spark 4.2. The JVM uses the public JTS IO implementation and Python uses Shapely WKB. The SQL transport preserves SRID and supported XY, XYZ, XYM and XYZM layouts, including dimensional empties. Python M support depends on the installed Shapely/GEOS version.

This changes the internal payload of `GeometryUDT`; it does not change its SQL type name or the Python Shapely geometry API. The JVM writer retains its existing layout validation: explicitly different layouts within a polygon or multipart geometry are rejected, while general geometry collection members can retain independent layouts. Python follows Shapely's public WKB representation, which can preserve mixed member layouts instead of applying the compact SQL serializer's single-layout promotion. When Shapely/GEOS retains the declared dimensions, an XY/XYZ `MultiPoint` remains distinct from an all-XYZ `MultiPoint` whose first Z is NaN in `geom_equals_identical`. GEOS versions before 3.12 can treat a NaN-Z member as XY before Sedona receives the value. The public JVM WKB writer canonicalizes NaN payload bits and represents a point whose X and Y are both NaN as `POINT EMPTY`. SRIDs use the full signed 32-bit EWKB field instead of the compact format's 24-bit field. Finite coordinates and supported coordinate dimensions are preserved.

Spark 4.2 instead uses Spark's native `GEOMETRY` and `GEOGRAPHY` SQL types and Spark's default spatial serializer. Their values and CRS validation follow Spark's native contract; the EWKB payload and layout rules above describe the explicit Sedona UDT path. Sedona still uses its S2 implementation for geography computation.

## Spark 4.2 native types

Use a Sedona version that provides the Spark 4.2 module, with Scala 2.13 and matching Spark/PySpark 4.2 versions. The artifact names are `sedona-spark-4.2_2.13` and `sedona-spark-shaded-4.2_2.13`; use the same Sedona version for the JVM artifact and Python package. A Spark 3.5, 4.0 or 4.1 artifact does not enable native integration on Spark 4.2. Managed Spark distributions require their own compatibility validation.

The Spark 4.2 module activates native spatial types automatically. Spark owns the native spatial serializer and CRS validation. Sedona uses JTS for geometry computation and its existing S2 implementation for geography computation. Its public EWKB-to-ISO header adapter prepares inputs for Spark's APIs; it does not replace Spark's native codec. Direct computational-object paths avoid redundant Sedona UDT byte round trips while retaining Spark's encoding and validation, including validation between nested spatial calls.

Sedona leaves these five functions registered to Spark's built-in implementations: `ST_AsBinary`, `ST_GeomFromWKB`, `ST_GeogFromWKB`, `ST_SRID` and `ST_SetSRID`. Their input validation, default SRIDs and output types follow Spark. Sedona's Python wrappers dispatch to those built-ins too. For example, use `ST_GeomFromEWKB` for EWKB input; Spark's `ST_GeomFromWKB` accepts ISO WKB.

### Geography CRS defaults

On Spark 4.2, geography constructors with an omitted CRS use SRID 4326. This includes text and GeoHash constructors and EWKT/EWKB with no embedded SRID. An explicitly supplied SRID is preserved for Spark validation: SRID 0 and projected CRSs such as 3857 are rejected for native geography. This does not change the S2 computational algorithms or older-runtime/explicit-UDT geography behavior.

The Python `to_spark_geography()` helper treats a Shapely SRID of 0 as unset and supplies 4326, including a Shapely value inside Sedona's `Geography` wrapper. That input representation does not distinguish an omitted SRID from an explicitly assigned zero. SQL constructors and existing Spark native values retain the explicit-SRID behavior above.

## Migrating persisted UDT values

An ordinary Parquet file written with the old `GeometryUDT` can contain Sedona's compact serialization bytes under UDT metadata. These bytes are not EWKB and are not automatically converted by the new reader. Export such data with the old Sedona version before upgrading. For example:

```python
# Run with the Sedona version that wrote the original UDT data.
old_df = sedona.read.parquet("old-udt-data")
old_df.selectExpr("id", "ST_AsEWKB(geom) AS geom_wkb").write.parquet(
    "portable-wkb-data"
)
```

Then load the portable binary column with the new version:

```python
portable_df = sedona.read.parquet("portable-wkb-data")
new_df = portable_df.selectExpr("id", "ST_GeomFromEWKB(geom_wkb) AS geom")
```

Use `ST_GeomFromEWKB` for this exported EWKB column on every supported Spark version. On Spark 4.2, `ST_GeomFromWKB` is Spark's built-in function and expects ISO WKB, not EWKB. Include the remaining non-geometry columns in the projection as appropriate. Check row counts, nulls, SRIDs and coordinate dimensions after migrating. Standard GeoParquet and explicit WKB/EWKB columns already use portable geometry encodings and do not need this compact-UDT conversion.

## Scope

The RDD/Kryo geometry and spatial-index serializers, Python RDD framing and Flink serialization retain their existing formats. Geography's S2 implementation and explicit geography UDT byte format are unchanged; the Spark 4.2 native boundary converts to and from Spark geography values.

## Python and explicit UDT schemas

On Spark 3.5–4.1, application code can pass Shapely objects through `GeometryType`. On Spark 4.2, Sedona's `GeometryType` and `GeographyType` names refer to Spark's native types. These helpers provide the Python boundary:

```python
from sedona.spark.sql.types import (
    geometry_type,
    geography_type,  # native ANY schemas on Spark 4.2
    to_spark_geometry,
    to_spark_geography,  # Python input values / UDF results
    to_shapely,  # collected native values
    LegacyGeometryType,
    LegacyGeographyType,  # explicit Sedona UDT schemas
)
```

Native conversion requires Shapely 2 or later. For classic PySpark `createDataFrame` with an explicit legacy schema and plain Shapely or Sedona `Geography` inputs, set `verifySchema=False`; the declared UDT still performs serialization. Use the conversion helpers for native input without changing global `__UDT__` registrations. On the JVM, explicit UDT schemas import `GeometryUDT` and `GeographyUDT` from `org.apache.spark.sql.sedona_sql.UDT`; they are distinct from Spark's native `org.apache.spark.sql.types` classes.

An explicit `GeometryUDT` uses the new EWKB payload on every supported Spark version, so choosing that type does not restore the old compact SQL bytes. Follow the migration steps above for persisted compact UDT data. The `geometry_serde` Python utilities retain their separate compact-format contract; their bytes are no longer the SQL geometry payload.

### Sedona GeoPandas boundaries

Local construction and CRS assignment in Sedona's `GeoSeries` and `GeoDataFrame` use native `GEOMETRY(ANY)`, retaining each value's SRID and per-column CRS metadata. CRS assignment changes the CRS declaration without transforming coordinates, as in GeoPandas. `to_geopandas()` collects through WKB/SRID columns to preserve null geometry, supported coordinate layouts and CRS. Selecting and collecting an ordinary column retains its pandas-on-Spark anchor and excludes unused geometry at collection.

These adapters belong to Sedona's GeoPandas API. They do not change Spark's global pandas-on-Spark or `toPandas()` implementations.

## Native file I/O CRS constraints

Spark 4.2 GeoParquet writes require each native geometry column to have a fixed `GEOMETRY(<srid>)` type, including empty and all-null columns. Spark does not support casting `GEOMETRY(ANY)` to a fixed spatial type. Construct a fixed-CRS column before writing, after checking that every non-null input has the intended CRS. A literal `ST_SetSRID` produces a fixed type but changes the CRS declaration without validating the previous SRID or transforming coordinates; it must not be used to hide mixed-CRS data. Independent geometry columns may use different CRSs. Native geometry must be a top-level field: geometry nested in a struct, array or map is rejected because the writer cannot preserve nested GeoParquet CRS metadata.

Schema inference checks the files for each inferred geometry column, even with `mergeSchema=false`. It produces a fixed native CRS and rejects conflicting per-file CRSs or non-null CRS definitions that cannot be represented by a native SRID. An explicit `GEOMETRY(ANY)` read schema can retain supported per-file SRIDs without forcing a common CRS; an explicit fixed type validates each file against that CRS. Explicit `GeometryUDT` reads retain their existing per-file CRS behavior. A scalar-only explicit schema does not require unused geometry CRS metadata to be representable.

GeoPackage's undefined Cartesian CRS identifier `-1` is not accepted by Spark's native geometry type. The Spark 4.2 GeoPackage reader rejects such values without changing their CRS to `0`. The older-runtime reader and explicit `GeometryReader.extractGeometry(bytes, GeometryUDT())` / `extractWKB(bytes)` utility paths preserve `-1`. The GeoPackage data source does not accept a user-supplied schema, so `read.schema(...)` is not an explicit-UDT workaround for that reader. Assign a valid, known CRS in the source data before loading it as native geometry.

## Known Spark 4.2 limitations

Native type support does not make every Spark operation spatially aware. The integration retains these upstream constraints:

* Generic `cache()` / `persist()` paths and grouping directly by native geometry or geography can fail. This port does not add general cache or grouping support for native spatial columns. Sedona algorithms may manage nonspatial IDs internally; that does not enable caching or grouping arbitrary user spatial columns.
* Raw Spark `DataFrame.pandas_api()` cannot infer pandas dtypes for native spatial columns. Use Sedona-owned GeoPandas constructors and `to_geopandas()` for those boundaries.
* PySpark's fixed-CRS schema mapping covers fewer CRSs than the JVM. A CRS accepted by the JVM, such as EPSG:26909, can fail when Python parses a fixed spatial schema. Native ANY schemas preserve per-value SRIDs and avoid that fixed-schema mapping at the Sedona GeoPandas boundary; they do not extend Spark's CRS validation.
* Raw native `toPandas()` has conversion limitations: the validated runtime fails without Arrow even for non-null geometry or geography, and its Arrow path fails when these columns contain nulls. Non-null Arrow controls pass. Sedona's `to_geopandas()` boundary handles WKB, integer SRIDs and nulls explicitly. Arrow-enabled tests with ordinary empty `NullType` columns may still use Spark's standard Arrow fallback; they are distinct from strict native spatial Arrow checks.

Focused Spark 4.2 tests cover JVM spatial boundaries and file I/O, native and explicit-UDT Python values in Classic and Connect, and Arrow/UDF conversions. This scope does not establish compatibility for every shared Sedona test, arbitrary pandas-on-Spark operation or managed Spark runtime. Consult the limitations above when upgrading an application.
