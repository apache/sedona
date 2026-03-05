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

# ST_NumPoints

Introduction: Returns number of points in a LineString

![ST_NumPoints](../../../../image/ST_NumPoints/ST_NumPoints.svg "ST_NumPoints")

Format: `ST_NumPoints(geom: geometry)`

Return type: `Integer`

!!!note
    If any other geometry is provided as an argument, an IllegalArgumentException is thrown.

SQL Example:

```
SELECT ST_NumPoints(ST_GeomFromWKT('MULTIPOINT ((0 0), (1 1), (0 1), (2 2))'))
```

Output:

```
IllegalArgumentException: Unsupported geometry type: MultiPoint, only LineString geometry is supported.
```

SQL Example:

```sql
SELECT ST_NumPoints(ST_GeomFromText('LINESTRING(0 1, 1 0, 2 0)'))
```

Output: `3`
