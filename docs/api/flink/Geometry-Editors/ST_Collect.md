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

Introduction: Collects spatial values without dissolving their boundaries. Homogeneous
`Point`, `LineString`, or `Polygon` values produce the corresponding multi-object; mixed types
produce a `GeometryCollection`.

![ST_Collect](../../../image/ST_Collect/ST_Collect.svg "ST_Collect")

Format:

`ST_Collect(geom1: Geometry, geom2: Geometry)`

`ST_Collect(geom: ARRAY[Geometry])`

`ST_Collect(geog1: Geography, geog2: Geography)`

`ST_Collect(geog: ARRAY[Geography])`

Return type: `Geometry` or `Geography`, matching the input type

Since: `v1.5.0` (`Geometry`), `v1.9.1` (`Geography`)

Member order and duplicates are retained. The `Geography` overloads ignore null elements and use
the first non-null input's SRID for the result.

Geometry example:

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

Geography array example:

```sql
SELECT ST_AsEWKT(
    ST_Collect(
        ARRAY[
            ST_GeogFromWKT('POINT(-122.4 37.8)', 4326),
            ST_GeogFromWKT('POINT(-122.5 37.7)', 4326)
        ]
    )
) AS geog
```

Result:

```
SRID=4326;MULTIPOINT ((-122.4 37.8), (-122.5 37.7))
```
