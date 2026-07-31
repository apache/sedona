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

# ST_Collect

Introduction: Collects spatial values into a multi-object or collection without dissolving their
boundaries. Homogeneous `Point`, `LineString`, or `Polygon` inputs produce the corresponding
multi-object; mixed input types produce the OGC/WKT `GeometryCollection` type.

![ST_Collect](../../../image/ST_Collect/ST_Collect.svg "ST_Collect")

Format:

`ST_Collect(*geom: Geometry)`

`ST_Collect(geom: ARRAY[Geometry])`

`ST_Collect(*geog: Geography)`

`ST_Collect(geog: ARRAY[Geography])`

Return type: `Geometry` or `Geography`, matching the input type

Since: `v1.2.0` (`Geometry`), `v1.9.1` (`Geography`)

Null elements are ignored. Member values and duplicates are preserved. For `Geography` inputs,
the first non-null input supplies the output SRID; later inputs are not required to have the same
SRID. In contrast, [`ST_Collect_Agg`](../Aggregate-Functions/ST_Collect_Agg.md) rejects a group
containing mixed Geography SRIDs.

SQL Example

```sql
SELECT ST_Collect(
    ST_GeomFromText('POINT(21.427834 52.042576573)'),
    ST_GeomFromText('POINT(45.342524 56.342354355)')
) AS geom
```

Result:

```
+---------------------------------------------------------------+
|geom                                                           |
+---------------------------------------------------------------+
|MULTIPOINT ((21.427834 52.042576573), (45.342524 56.342354355))|
+---------------------------------------------------------------+
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
SELECT region, ST_Collect(ARRAY_AGG(geog)) AS geographies
FROM observations
GROUP BY region
```

SQL Example

```sql
SELECT ST_Collect(
    Array(
        ST_GeomFromText('POINT(21.427834 52.042576573)'),
        ST_GeomFromText('POINT(45.342524 56.342354355)')
    )
) AS geom
```

Result:

```
+---------------------------------------------------------------+
|geom                                                           |
+---------------------------------------------------------------+
|MULTIPOINT ((21.427834 52.042576573), (45.342524 56.342354355))|
+---------------------------------------------------------------+
```
