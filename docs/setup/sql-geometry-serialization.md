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

# Spark SQL geometry serialization

Starting in v2.0.0, Spark SQL geometry values use standard EWKB internally. The JVM uses the public JTS IO implementation and Python uses Shapely WKB. The SQL transport preserves SRID and supported XY, XYZ, XYM and XYZM layouts, including dimensional empties. Python M support depends on the installed Shapely/GEOS version.

This changes the internal payload of `GeometryUDT`; it does not change its SQL type name or the Python Shapely geometry API. The JVM writer retains its existing layout validation: explicitly different layouts within a polygon or multipart geometry are rejected, while general geometry collection members can retain independent layouts. Python follows Shapely's public WKB representation, which can preserve mixed member layouts instead of applying the compact SQL serializer's single-layout promotion. For example, an XY/XYZ `MultiPoint` remains distinct from an all-XYZ `MultiPoint` whose first Z is NaN in `geom_equals_identical`. The public JVM WKB writer canonicalizes NaN payload bits and represents a point whose X and Y are both NaN as `POINT EMPTY`. SRIDs use the full signed 32-bit EWKB field instead of the compact format's 24-bit field. Finite coordinates and supported coordinate dimensions are preserved.

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
new_df = portable_df.selectExpr("id", "ST_GeomFromWKB(geom_wkb) AS geom")
```

Include the remaining non-geometry columns in the projection as appropriate. Check row counts, nulls, SRIDs and coordinate dimensions after migrating. Standard GeoParquet and explicit WKB/EWKB columns already use portable geometry encodings and do not need this compact-UDT conversion.

## Scope

The RDD/Kryo geometry and spatial-index serializers, Python RDD framing and Flink serialization retain their existing formats. Geography's S2 implementation and geography UDT byte format are also unchanged by this geometry-only migration.

Application code should pass Shapely objects through `GeometryType` or use SQL WKB functions at external boundaries. The legacy `geometry_serde` utilities remain available for their existing compact-format contract; their bytes are no longer the SQL geometry payload.
