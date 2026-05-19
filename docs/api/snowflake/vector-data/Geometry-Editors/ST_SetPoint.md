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

# ST_SetPoint

Introduction: Replace Nth point of linestring with given point. Index is 0-based. Negative index are counted backwards, e.g., -1 is last point.

![ST_SetPoint](../../../../image/ST_SetPoint/ST_SetPoint.svg "ST_SetPoint")

Format: `ST_SetPoint (linestring: geometry, index: integer, point: geometry)`

Return type: `Geometry`

Example:

```sql
SELECT ST_SetPoint(ST_GeomFromText('LINESTRING (0 0, 0 1, 1 1)'), 2, ST_GeomFromText('POINT (1 0)')) AS geom
```

Result:

```
+--------------------------+
|geom                      |
+--------------------------+
|LINESTRING (0 0, 0 1, 1 0)|
+--------------------------+
```
