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

Format: `ST_SharedPaths(A: geometry, B: geometry)`

Return type: `Geometry`

This function supports Snowflake `GEOMETRY` values. It is not available for `GEOGRAPHY` values.
Both inputs must be a `LineString` or `MultiLineString`. The result is a `GeometryCollection`
containing exactly two `MultiLineString` elements. The first contains paths traversed in the same
direction by both inputs; the second contains paths traversed in opposite directions. Coordinates
in both elements follow the direction of `A`.

For non-simple inputs that traverse the same path more than once, direction follows the first
matching source traversal. Consequently, swapping `A` and `B` can change which result element
contains a path.

Snowflake's native `GEOMETRY` bridge passes values to Sedona as GeoJSON, which does not carry SRID
metadata. Consequently, this variant cannot reject mixed input SRIDs or preserve a matching SRID;
its result has SRID 0. The legacy binary geometry interface preserves matching SRIDs. A non-empty
result retains Z only when at least one input has Z and every coordinate of every returned shared
path resolves to a finite Z from the input geometries. If any returned coordinate does not resolve
to a finite source Z, the entire result is two-dimensional. M values are always dropped. A result
with no shared paths is also two-dimensional.

When the inputs do not share a path, including when they intersect only at points, both elements
are empty `MultiLineString` geometries.

## Visual examples

### Same and opposite directions

Input `B` first follows `A`, leaves it, and later traverses another part of `A` backwards. The
single result therefore populates both direction buckets. Notice that the path in element 1 is
reoriented to follow `A`, even though `B` traverses it in the opposite direction.

![Same-direction and opposite-direction paths returned together by ST_SharedPaths](../../../../image/ST_SharedPaths/ST_SharedPaths.svg "Both ST_SharedPaths result elements populated")

### Multipart input and noded output

Input `A` can be a `MultiLineString`. In this example, `B` shares part of two different components
of `A`. The vertex at `(90 161)` belongs to `B` and splits the shared diagonal into two paths in
element 0.

![ST_SharedPaths over multipart input with a shared path split at an input vertex](../../../../image/ST_SharedPaths/ST_SharedPaths_multipart.svg "Multipart and noded ST_SharedPaths result")

### Point-only intersections

Crossing at an interior point and touching at an endpoint are both zero-dimensional contacts.
Neither is a shared path, so both `MultiLineString` elements are empty.

![ST_SharedPaths ignores interior crossings and endpoint-only touches](../../../../image/ST_SharedPaths/ST_SharedPaths_point_intersections.svg "Point-only intersections are not shared paths")

## Opposite-direction SQL example

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
