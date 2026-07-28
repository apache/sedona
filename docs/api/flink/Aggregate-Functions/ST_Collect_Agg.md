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

Introduction: Collects all non-null spatial values in a group without dissolving their
boundaries. Homogeneous `Point`, `LineString`, or `Polygon` values produce the corresponding
multi-object; mixed types produce a `GeometryCollection`.

![ST_Collect_Agg](../../../image/ST_Collect_Agg/ST_Collect_Agg.svg "ST_Collect_Agg")

Format:

`ST_Collect_Agg (A: Geometry)`

`ST_Collect_Agg (A: Geography)`

Return type: `Geometry` or `Geography`, matching the input type

Since: `v1.9.1`

Null values are ignored and duplicates are preserved. The function returns `NULL` when a group
has no non-null values. All `Geography` values in a group must have the same SRID.

`ST_Collect_Aggr` is also registered as an alias.

Geometry example:

```sql
SELECT ST_Collect_Agg(geom)
FROM (
    VALUES
        (ST_GeomFromWKT('POINT(1 2)')),
        (ST_GeomFromWKT('POINT(3 4)')),
        (ST_GeomFromWKT('POINT(1 2)'))
) AS observations(geom)
```

Output:

```
MULTIPOINT ((1 2), (3 4), (1 2))
```

Geography example:

```sql
SELECT region, ST_Collect_Agg(geog) AS geographies
FROM observations
GROUP BY region
```
