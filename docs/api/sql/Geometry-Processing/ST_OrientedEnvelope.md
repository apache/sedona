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

# ST_OrientedEnvelope

Introduction: Returns the minimum-area rotated rectangle enclosing a geometry. The rectangle may be rotated relative to the coordinate axes. Degenerate inputs may result in a Point or LineString being returned.

Format: `ST_OrientedEnvelope(geom: Geometry)`

Since: `v1.8.1`

SQL Example

```sql
SELECT ST_OrientedEnvelope(ST_GeomFromWKT('POLYGON ((0 0, 1 0, 5 4, 4 4, 0 0))'))
```

Output:

```
POLYGON ((0 0, 4.5 4.5, 5 4, 0.5 -0.5, 0 0))
```
