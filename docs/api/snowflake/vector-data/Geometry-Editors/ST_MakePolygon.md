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

# ST_MakePolygon

Introduction: Function to convert closed linestring to polygon including holes. The holes must be a MultiLinestring. If holes are provided, they should be fully contained within the shell. Holes outside the shell will produce an invalid polygon (matching PostGIS behavior). Use `ST_IsValid` to check the result.

![ST_MakePolygon](../../../../image/ST_MakePolygon/ST_MakePolygon.svg "ST_MakePolygon")

Format: `ST_MakePolygon(geom: geometry, holes: <geometry>)`

Return type: `Geometry`

Example:

Query:

```sql
SELECT
    ST_MakePolygon(
        ST_GeomFromText('LINESTRING(0 0, 10 0, 10 10, 0 10, 0 0)'),
        ST_GeomFromText('MultiLINESTRING((2 2, 4 2, 4 4, 2 4, 2 2))')
    ) AS polygon
```

Result:

```
+--------------------------------------------------------------------+
|polygon                                                              |
+--------------------------------------------------------------------+
|POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2))|
+--------------------------------------------------------------------+

```
