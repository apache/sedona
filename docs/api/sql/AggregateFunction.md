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

## ST_Collect_Agg

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

## ST_Envelope_Agg

Introduction: Return the entire envelope boundary of all geometries in A. Empty geometries and null values are skipped. If all inputs are empty or null, the result is null. This behavior is consistent with PostGIS's `ST_Extent`.

Format: `ST_Envelope_Agg (A: geometryColumn)`

Since: `v1.0.0`

!!!note
    This function was previously named `ST_Envelope_Aggr`, which is deprecated since `v1.8.1`.

SQL Example

```sql
SELECT ST_Envelope_Agg(ST_GeomFromText('MULTIPOINT(1.1 101.1,2.1 102.1,3.1 103.1,4.1 104.1,5.1 105.1,6.1 106.1,7.1 107.1,8.1 108.1,9.1 109.1,10.1 110.1)'))
```

Output:

```
POLYGON ((1.1 101.1, 1.1 120.1, 20.1 120.1, 20.1 101.1, 1.1 101.1))
```

## ST_Intersection_Agg

Introduction: Return the polygon intersection of all polygons in A

Format: `ST_Intersection_Agg (A: geometryColumn)`

Since: `v1.0.0`

!!!note
    This function was previously named `ST_Intersection_Aggr`, which is deprecated since `v1.8.1`.

SQL Example

```sql
SELECT ST_Intersection_Agg(ST_GeomFromText('MULTIPOINT(1.1 101.1,2.1 102.1,3.1 103.1,4.1 104.1,5.1 105.1,6.1 106.1,7.1 107.1,8.1 108.1,9.1 109.1,10.1 110.1)'))
```

Output:

```
MULTIPOINT ((1.1 101.1), (2.1 102.1), (3.1 103.1), (4.1 104.1), (5.1 105.1), (6.1 106.1), (7.1 107.1), (8.1 108.1), (9.1 109.1), (10.1 110.1))
```

## ST_Union_Agg

Introduction: Return the polygon union of all polygons in A

Format: `ST_Union_Agg (A: geometryColumn)`

Since: `v1.0.0`

!!!note
    This function was previously named `ST_Union_Aggr`, which is deprecated since `v1.8.1`.

SQL Example

```sql
SELECT ST_Union_Agg(ST_GeomFromText('MULTIPOINT(1.1 101.1,2.1 102.1,3.1 103.1,4.1 104.1,5.1 105.1,6.1 106.1,7.1 107.1,8.1 108.1,9.1 109.1,10.1 110.1)'))
```

Output:

```
MULTIPOINT ((1.1 101.1), (2.1 102.1), (3.1 103.1), (4.1 104.1), (5.1 105.1), (6.1 106.1), (7.1 107.1), (8.1 108.1), (9.1 109.1), (10.1 110.1))
```
