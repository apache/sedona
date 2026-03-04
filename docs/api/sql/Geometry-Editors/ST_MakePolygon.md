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

Introduction: Function to convert closed linestring to polygon including holes. If holes are provided, they should be fully contained within the shell. Holes outside the shell will produce an invalid polygon (matching PostGIS behavior). Use `ST_IsValid` to check the result.

Format: `ST_MakePolygon(geom: Geometry, holes: ARRAY[Geometry])`

Return type: `Geometry`

Since: `v1.1.0`

SQL Example

```sql
SELECT ST_MakePolygon(
        ST_GeomFromText('LINESTRING(0 0, 10 0, 10 10, 0 10, 0 0)'),
        ARRAY(ST_GeomFromText('LINESTRING(2 2, 4 2, 4 4, 2 4, 2 2)'))
    )
```

Output:

```
POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2))
```
