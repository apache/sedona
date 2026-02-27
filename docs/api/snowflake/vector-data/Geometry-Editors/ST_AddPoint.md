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

# ST_AddPoint

Introduction: RETURN Linestring with additional point at the given index, if position is not available the point will be added at the end of line.

Format: `ST_AddPoint(geom: geometry, point: geometry, position: integer)`

Format: `ST_AddPoint(geom: geometry, point: geometry)`

SQL example:

```sql
SELECT ST_AddPoint(ST_GeomFromText('LINESTRING(0 0, 1 1, 1 0)'), ST_GeomFromText('Point(21 52)'), 1)

SELECT ST_AddPoint(ST_GeomFromText('Linestring(0 0, 1 1, 1 0)'), ST_GeomFromText('Point(21 52)'))
```

Output:

```
LINESTRING(0 0, 21 52, 1 1, 1 0)
LINESTRING(0 0, 1 1, 1 0, 21 52)
```
