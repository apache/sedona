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

# ST_MakeValid

Introduction: Given an invalid geometry, create a valid representation of the geometry.

Collapsed geometries are either converted to empty (keepCollapsed=false) or a valid geometry of lower dimension (keepCollapsed=true).
Default is keepCollapsed=false.

Format:

`ST_MakeValid (A: Geometry)`

`ST_MakeValid (A: Geometry, keepCollapsed: Boolean)`

Since: `v1.0.0`

SQL Example

```sql
WITH linestring AS (
    SELECT ST_GeomFromWKT('LINESTRING(1 1, 1 1)') AS geom
) SELECT ST_MakeValid(geom), ST_MakeValid(geom, true) FROM linestring
```

Result:

```
+------------------+------------------------+
|st_makevalid(geom)|st_makevalid(geom, true)|
+------------------+------------------------+
|  LINESTRING EMPTY|             POINT (1 1)|
+------------------+------------------------+
```

!!!note
    In Sedona up to and including version 1.2 the behaviour of ST_MakeValid was different.
Be sure to check you code when upgrading. The previous implementation only worked for (multi)polygons and had a different interpretation of the second, boolean, argument.
It would also sometimes return multiple geometries for a single geometry input.
