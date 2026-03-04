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

# ST_ForceCollection

Introduction: This function converts the input geometry into a GeometryCollection, regardless of the original geometry type. If the input is a multipart geometry, such as a MultiPolygon or MultiLineString, it will be decomposed into a GeometryCollection containing each individual Polygon or LineString element from the original multipart geometry.

![ST_ForceCollection](../../../../image/ST_ForceCollection/ST_ForceCollection.svg "ST_ForceCollection")
Format: `ST_ForceCollection(geom: Geometry)`

Return type: `Geometry`

SQL Example

```sql
SELECT ST_ForceCollection(
            ST_GeomFromWKT(
                "MULTIPOINT (30 10, 40 40, 20 20, 10 30, 10 10, 20 50)"
    )
)
```

Output:

```
GEOMETRYCOLLECTION (POINT (30 10), POINT (40 40), POINT (20 20), POINT (10 30), POINT (10 10), POINT (20 50))
```
