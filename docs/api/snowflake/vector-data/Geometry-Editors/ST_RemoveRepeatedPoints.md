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

# ST_RemoveRepeatedPoints

Introduction: This function eliminates consecutive duplicate points within a geometry, preserving endpoints of LineStrings. It operates on (Multi)LineStrings, (Multi)Polygons, and MultiPoints, processing GeometryCollection elements individually. When an optional 'tolerance' value is provided, vertices within that distance are also considered duplicates.

![ST_RemoveRepeatedPoints](../../../../image/ST_RemoveRepeatedPoints/ST_RemoveRepeatedPoints.svg "ST_RemoveRepeatedPoints")

Format:

`ST_RemoveRepeatedPoints(geom: Geometry, tolerance: Double)`

`ST_RemoveRepeatedPoints(geom: Geometry)`

Return type: `Geometry`

SQL Example:

```sql
SELECT ST_RemoveRepeatedPoints(
        ST_GeomFromWKT('MULTIPOINT ((20 20), (10 10), (30 30), (40 40), (20 20), (30 30), (40 40))')
       )
```

Output:

```
MULTIPOINT ((20 20), (10 10), (30 30), (40 40))
```

SQL Example:

```sql
SELECT ST_RemoveRepeatedPoints(
        ST_GeomFromWKT('LINESTRING (20 20, 10 10, 30 30, 40 40, 20 20, 30 30, 40 40)')
       )
```

Output:

```
LINESTRING (20 20, 10 10, 30 30, 40 40, 20 20, 30 30, 40 40)
```

SQL Example: Each geometry within a collection is processed independently.

```sql
ST_RemoveRepeatedPoints(
        ST_GeomFromWKT('GEOMETRYCOLLECTION (POINT (10 10), POINT(10 10), LINESTRING (20 20, 20 20, 30 30, 30 30), MULTIPOINT ((80 80), (90 90), (90 90), (100 100)))')
    )
```

Output:

```
GEOMETRYCOLLECTION (POINT (10 10), POINT (10 10), LINESTRING (20 20, 30 30), MULTIPOINT ((80 80), (90 90), (100 100)))
```

SQL Example: Elimination of repeated points within a specified distance tolerance.

```sql
SELECT ST_RemoveRepeatedPoints(
        ST_GeomFromWKT('LINESTRING (20 20, 10 10, 30 30, 40 40, 20 20, 30 30, 40 40)'),
        20
       )
```

Output:

```
LINESTRING (20 20, 40 40, 20 20, 40 40)
```
