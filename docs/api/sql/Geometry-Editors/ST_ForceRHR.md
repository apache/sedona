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

# ST_ForceRHR

Introduction: Sets the orientation of polygon vertex orderings to follow the Right-Hand-Rule convention. The exterior ring will have a clockwise winding order, while any interior rings are oriented counter-clockwise. This ensures the area bounded by the polygon falls on the right-hand side relative to the ring directions. The function is an alias for [ST_ForcePolygonCW](ST_ForcePolygonCW.md).

![ST_ForceRHR](../../../image/ST_ForceRHR/ST_ForceRHR.svg "ST_ForceRHR")

Format: `ST_ForceRHR(geom: Geometry)`

Return type: `Geometry`

Since: `v1.6.1`

SQL Example:

```sql
SELECT ST_AsText(ST_ForceRHR(ST_GeomFromText('POLYGON ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20))')))
```

Output:

```
POLYGON ((20 35, 45 20, 30 5, 10 10, 10 30, 20 35), (30 20, 20 25, 20 15, 30 20))
```
