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

# ST_Collect_Agg

Introduction: Collects all non-null spatial values in a column into a single multi-object or
collection. Unlike `ST_Union_Agg`, this function does not dissolve boundaries. Homogeneous
`Point`, `LineString`, or `Polygon` inputs produce the corresponding multi-object; mixed input
types produce the OGC/WKT `GeometryCollection` type.

![ST_Collect_Agg](../../../image/ST_Collect_Agg/ST_Collect_Agg.svg "ST_Collect_Agg")

Format:

`ST_Collect_Agg (A: Geometry)`

`ST_Collect_Agg (A: Geography)`

Return type: `Geometry` or `Geography`, matching the input type

Since: `v1.8.1` (`Geometry`), `v1.9.1` (`Geography`)

Duplicates are preserved. The function returns `NULL` when a group has no non-null values.
All non-null `Geography` values in a group must have the same SRID; a group containing mixed
SRIDs is rejected. In contrast, scalar [`ST_Collect`](../Geometry-Editors/ST_Collect.md) uses the
first non-null Geography input's SRID for the output.

SQL Example

```sql
SELECT ST_Collect_Agg(geom) FROM (
  SELECT ST_GeomFromWKT('POINT(1 2)') AS geom
  UNION ALL
  SELECT ST_GeomFromWKT('POINT(3 4)') AS geom
  UNION ALL
  SELECT ST_GeomFromWKT('POINT(5 6)') AS geom
)
```

Output:

```
MULTIPOINT ((1 2), (3 4), (5 6))
```

SQL Example with GROUP BY

```sql
SELECT category, ST_Collect_Agg(geom) FROM geometries GROUP BY category
```

Geography SQL Example with GROUP BY

```sql
WITH observations AS (
    SELECT 'west' AS region, ST_GeogFromWKT('POINT(-122.4 37.8)', 4326) AS geog
    UNION ALL
    SELECT 'west' AS region, ST_GeogFromWKT('POINT(-122.5 37.7)', 4326) AS geog
    UNION ALL
    SELECT 'east' AS region, ST_GeogFromWKT('POINT(-73.9 40.7)', 4326) AS geog
)
SELECT region, ST_Collect_Agg(geog) AS geographies
FROM observations
GROUP BY region
```
