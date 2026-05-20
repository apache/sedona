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

# RS_DWithin

Introduction: Returns true if the raster or geometry on the left side is within `distance` **meters** of the raster or geometry on the right side. The convex hull of the raster is considered in the test. At least one of the two shape arguments must be a raster — for the geometry-only case use [`ST_DWithin`](../Predicates/ST_DWithin.md) instead.

Rules for testing spatial relationship:

- If the raster or geometry does not have a defined SRID, it is assumed to be in WGS84.
- Both sides are unconditionally projected to WGS84 before the test, so `distance` is **always** measured in meters regardless of the input CRS.
- The per-row test computes the **minimum geodesic distance** between the two shapes — the raster is represented by its convex hull, and the geodesic distance is measured between the closest pair of points on the two hulls (not centroid-to-centroid). Two raster footprints that overlap or touch therefore satisfy `RS_DWithin(a, b, 0)`, mirroring [`RS_Intersects`](RS_Intersects.md).

When used as a join condition, Sedona plans the join as an optimized distance join (`BroadcastIndexJoinExec` or `DistanceJoinExec`): each side's WGS84 envelope is computed, the distance-side envelope is expanded by `distance` meters using the Haversine polar-radius approximation (the same expansion `ST_DistanceSphere` uses), and the resulting envelopes drive an R-tree filter before the per-row `RS_DWithin` check refines the result.

Format:

`RS_DWithin(raster: Raster, geom: Geometry, distance: Double)`

`RS_DWithin(geom: Geometry, raster: Raster, distance: Double)`

`RS_DWithin(raster0: Raster, raster1: Raster, distance: Double)`

Return type: `Boolean`

Since: `v1.9.1`

SQL Example

```sql
SELECT RS_DWithin(
    RS_MakeEmptyRaster(1, 20, 20, 2, 22, 1),
    ST_SetSRID(ST_PolygonFromEnvelope(30, 30, 40, 40), 4326),
    5000000.0  -- 5 000 km, in meters
) AS within_5000km
```

Output:

```
+-------------+
|within_5000km|
+-------------+
|         true|
+-------------+
```

Using `RS_DWithin` as a distance-join condition (`distance` in meters):

```sql
SELECT r.id, p.id
FROM rasters r
JOIN points p ON RS_DWithin(r.raster, p.geom, 1000)  -- within 1 km
```
