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

# ST_CollectionExtract

Introduction: Returns a homogeneous multi-geometry from a given geometry collection.

The type numbers are:

1. POINT
2. LINESTRING
3. POLYGON

If the type parameter is omitted a multi-geometry of the highest dimension is returned.

Format: `ST_CollectionExtract (A:geometry)`

Format: `ST_CollectionExtract (A:geometry, type:Int)`

Example:

```sql
WITH test_data as (
    ST_GeomFromText(
        'GEOMETRYCOLLECTION(POINT(40 10), POLYGON((0 0, 0 5, 5 5, 5 0, 0 0)))'
    ) as geom
)
SELECT ST_CollectionExtract(geom) as c1, ST_CollectionExtract(geom, 1) as c2
FROM test_data

```

Result:

```
+----------------------------------------------------------------------------+
|c1                                        |c2                               |
+----------------------------------------------------------------------------+
|MULTIPOLYGON(((0 0, 0 5, 5 5, 5 0, 0 0))) |MULTIPOINT(40 10)                |              |
+----------------------------------------------------------------------------+
```
