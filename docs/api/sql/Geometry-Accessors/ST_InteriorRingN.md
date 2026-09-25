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

# ST_InteriorRingN

Introduction: Returns the Nth (1-based) interior linestring ring of the polygon geometry. Returns NULL if the geometry is not a polygon or the given N is out of range

!!!Note
    Since `v2.0.0`, `n` is 1-based, as in PostGIS: `n = 1` returns the first interior ring, and `0` or a negative `n` returns null. Earlier versions read `n` 0-based.

![ST_InteriorRingN](../../../image/ST_InteriorRingN/ST_InteriorRingN.svg "ST_InteriorRingN")

Format: `ST_InteriorRingN(geom: Geometry, n: Integer)`

Return type: `Geometry`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_InteriorRingN(ST_GeomFromText('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1), (1 3, 2 3, 2 4, 1 4, 1 3), (3 3, 4 3, 4 4, 3 4, 3 3))'), 1)
```

Output:

```
LINESTRING (1 1, 2 1, 2 2, 1 2, 1 1)
```
