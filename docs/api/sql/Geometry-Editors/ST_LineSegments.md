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

# ST_LineSegments

Introduction: This function transforms a LineString containing multiple coordinates into an array of LineStrings, each with precisely two coordinates. The `lenient` argument, true by default, prevents an exception from being raised if the input geometry is not a LineString.

Format:

`ST_LineSegments(geom: Geometry, lenient: Boolean)`

`ST_LineSegments(geom: Geometry)`

Return type: `Array<Geometry>`

Since: `v1.7.1`

SQL Example:

```sql
SELECT ST_LineSegments(
        ST_GeomFromWKT('LINESTRING(0 0, 10 10, 20 20, 30 30, 40 40, 50 50)'),
       false
    )
```

Output:

```
[LINESTRING (0 0, 10 10), LINESTRING (10 10, 20 20), LINESTRING (20 20, 30 30), LINESTRING (30 30, 40 40), LINESTRING (40 40, 50 50)]
```

SQL Example:

```sql
SELECT ST_LineSegments(
        ST_GeomFromWKT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')
    )
```

Output:

```
[]
```
