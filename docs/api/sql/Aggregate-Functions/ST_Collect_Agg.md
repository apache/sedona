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

# ST_Collect_Agg

Introduction: Collects all geometries in a geometry column into a single multi-geometry (MultiPoint, MultiLineString, MultiPolygon, or GeometryCollection). Unlike `ST_Union_Agg`, this function does not dissolve boundaries between geometries - it simply collects them into a multi-geometry.

Format: `ST_Collect_Agg (A: geometryColumn)`

Since: `v1.8.1`

SQL Example

```sql
SELECT ST_Collect_Agg(geom) FROM (
  SELECT ST_GeomFromWKT('POINT(1 2)') AS geom
  UNION ALL
  SELECT ST_GeomFromWKT('POINT(3 4)') AS geom
  UNION ALL
  SELECT ST_GeomFromWKT('POINT(5 6)') AS geom
)
```

Output:

```
MULTIPOINT ((1 2), (3 4), (5 6))
```

SQL Example with GROUP BY

```sql
SELECT category, ST_Collect_Agg(geom) FROM geometries GROUP BY category
```
