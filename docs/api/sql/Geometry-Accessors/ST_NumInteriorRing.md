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

# ST_NumInteriorRing

Introduction: Returns number of interior rings of polygon geometries. It is an alias of [ST_NumInteriorRings](ST_NumInteriorRings.md).

![ST_NumInteriorRing](../../../image/ST_NumInteriorRing/ST_NumInteriorRing.svg "ST_NumInteriorRing")

Format: `ST_NumInteriorRing(geom: Geometry)`

Return type: `Integer`

Since: `v1.6.1`

SQL Example

```sql
SELECT ST_NumInteriorRing(ST_GeomFromText('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))'))
```

Output:

```
1
```
