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

# ST_ShortestLine

Introduction: Returns the shortest LineString between two geometries. The line starts on geom1 and ends on geom2. If either geometry is empty, the function returns null.

![ST_ShortestLine](../../../../image/ST_ShortestLine/ST_ShortestLine.svg "ST_ShortestLine")

Format: `ST_ShortestLine(geom1: Geometry, geom2: Geometry)`

Return type: `Geometry`

SQL Example:

![ST_ShortestLine Point to Point](../../../../image/ST_ShortestLine/ST_ShortestLine_point_point.svg "ST_ShortestLine Point to Point")

```sql
SELECT ST_ShortestLine(
        ST_GeomFromText('POINT (0 0)'),
        ST_GeomFromText('POINT (3 4)')
)
```

Output:

```
LINESTRING (0 0, 3 4)
```

SQL Example:

![ST_ShortestLine Point to LineString](../../../../image/ST_ShortestLine/ST_ShortestLine_point_linestring.svg "ST_ShortestLine Point to LineString")

```sql
SELECT ST_ShortestLine(
        ST_GeomFromText('POINT (0 1)'),
        ST_GeomFromText('LINESTRING (0 0, 1 0, 2 0, 3 0, 4 0, 5 0)')
)
```

Output:

```
LINESTRING (0 1, 0 0)
```
