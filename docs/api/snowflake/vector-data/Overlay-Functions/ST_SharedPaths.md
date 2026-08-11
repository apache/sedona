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

# ST_SharedPaths

Introduction: Returns the paths shared by two lineal geometries, grouped by traversal direction.

![ST_SharedPaths](../../../../image/ST_SharedPaths/ST_SharedPaths.svg "ST_SharedPaths")

Format: `ST_SharedPaths(A: geometry, B: geometry)`

Return type: `Geometry`

This function supports Snowflake `GEOMETRY` values. It is not available for `GEOGRAPHY` values.
Both inputs must be a `LineString` or `MultiLineString`. The result is a `GeometryCollection`
containing exactly two `MultiLineString` elements. The first contains paths traversed in the same
direction by both inputs; the second contains paths traversed in opposite directions. Coordinates
in both elements follow the direction of `A`.

Snowflake's native `GEOMETRY` bridge passes values to Sedona as GeoJSON, which does not carry SRID
metadata. Consequently, this variant cannot reject mixed input SRIDs or preserve a matching SRID;
its result has SRID 0. The legacy binary geometry interface preserves matching SRIDs. As in
PostGIS, a non-empty result retains a Z dimension when either input has Z, while M values are
dropped. An empty result is two-dimensional.

When the inputs do not share a path, including when they intersect only at points, both elements
are empty `MultiLineString` geometries.

Example:

```sql
SELECT ST_SharedPaths(
    ST_GeometryFromWKT('LINESTRING (0 0, 10 0)'),
    ST_GeometryFromWKT('LINESTRING (15 0, 5 0)')
)
```

Result:

```
GEOMETRYCOLLECTION (MULTILINESTRING EMPTY, MULTILINESTRING ((5 0, 10 0)))
```
