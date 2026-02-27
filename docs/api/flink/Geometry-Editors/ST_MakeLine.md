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

# ST_MakeLine

Introduction: Creates a LineString containing the points of Point, MultiPoint, or LineString geometries. Other geometry types cause an error.

Format:

`ST_MakeLine(geom1: Geometry, geom2: Geometry)`

`ST_MakeLine(geoms: ARRAY[Geometry])`

Since: `v1.5.0`

Example:

```sql
SELECT ST_AsText( ST_MakeLine(ST_Point(1,2), ST_Point(3,4)) );
```

Output:

```
LINESTRING(1 2,3 4)
```

Example:

```sql
SELECT ST_AsText( ST_MakeLine( 'LINESTRING(0 0, 1 1)', 'LINESTRING(2 2, 3 3)' ) );
```

Output:

```
 LINESTRING(0 0,1 1,2 2,3 3)
```
