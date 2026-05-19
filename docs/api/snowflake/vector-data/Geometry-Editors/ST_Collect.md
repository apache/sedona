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

Introduction:

Build an appropriate `Geometry`, `MultiGeometry`, or `GeometryCollection` to contain the `Geometry`s in it. For example:

- If `geomList` contains a single `Polygon`, the `Polygon` is returned.
- If `geomList` contains several `Polygon`s, a `MultiPolygon` is returned.
- If `geomList` contains some `Polygon`s and some `LineString`s, a `GeometryCollection` is returned.
- If `geomList` is empty, an empty `GeometryCollection` is returned.

Note that this method does not "flatten" Geometries in the input, and hence if any MultiGeometries are contained in the input, a GeometryCollection containing them will be returned.

![ST_Collect](../../../../image/ST_Collect/ST_Collect.svg "ST_Collect")

Format

`ST_Collect(*geom: geometry)`

Return type: `Geometry`

Example:

```sql
WITH src_tbl AS (
    SELECT sedona.ST_GeomFromText('POINT (40 10)') AS geom
    UNION
    SELECT sedona.ST_GeomFromText('LINESTRING (0 5, 0 10)') AS geom
)
SELECT sedona.ST_AsText(collection)
FROM src_tbl,
     TABLE(sedona.ST_Collect(src_tbl.geom) OVER (PARTITION BY 1));
```

Result:

```
GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (0 5, 0 10))
```
