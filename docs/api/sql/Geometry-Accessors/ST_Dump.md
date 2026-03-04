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

# ST_Dump

Introduction: It expands the geometries. If the geometry is simple (Point, Polygon Linestring etc.) it returns the geometry
itself, if the geometry is collection or multi it returns record for each of collection components.

Format: `ST_Dump(geom: Geometry)`

Return type: `Array<Geometry>`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_Dump(ST_GeomFromText('MULTIPOINT ((10 40), (40 30), (20 20), (30 10))'))
```

Output:

```
[POINT (10 40), POINT (40 30), POINT (20 20), POINT (30 10)]
```
