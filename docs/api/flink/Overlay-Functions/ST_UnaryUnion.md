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

# ST_UnaryUnion

Introduction: This variant of [ST_Union](ST_Union.md) operates on a single geometry input. The input geometry can be a simple Geometry type, a MultiGeometry, or a GeometryCollection. The function calculates the geometric union across all components and elements within the provided geometry object.

Format: `ST_UnaryUnion(geometry: Geometry)`

Since: `v1.6.1`

SQL Example

```sql
SELECT ST_UnaryUnion(ST_GeomFromWKT('MULTIPOLYGON(((0 10,0 30,20 30,20 10,0 10)),((10 0,10 20,30 20,30 0,10 0)))'))
```

Output:

```
POLYGON ((10 0, 10 10, 0 10, 0 30, 20 30, 20 20, 30 20, 30 0, 10 0))
```
