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

!!!note
Please always keep the schema name `SEDONA` (e.g., `SEDONA.ST_GeomFromWKT`) when you use Sedona functions to avoid conflicting with Snowflake's built-in functions.

## ST_Envelope_Aggr

Introduction: Return the entire envelope boundary of all geometries in A

Format: `ST_Envelope_Aggr (A:geometryColumn)`

SQL example:

```sql
WITH src_tbl AS (
    SELECT sedona.ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS geom
    UNION
    SELECT sedona.ST_GeomFromText('POLYGON ((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))') AS geom
)
SELECT sedona.ST_AsText(envelope)
FROM src_tbl,
     TABLE(sedona.ST_Envelope_Aggr(src_tbl.geom) OVER (PARTITION BY 1));
```

Output:

```
POLYGON ((0 0, 0 1.5, 1.5 1.5, 1.5 0, 0 0))
```

## ST_Intersection_Aggr

Introduction: Return the polygon intersection of all polygons in A

Format: `ST_Intersection_Aggr (A:geometryColumn)`

SQL example:

```sql
WITH src_tbl AS (
    SELECT sedona.ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS geom
    UNION
    SELECT sedona.ST_GeomFromText('POLYGON ((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))') AS geom
)
SELECT sedona.ST_AsText(intersected)
FROM src_tbl,
     TABLE(sedona.ST_Intersection_Aggr(src_tbl.geom) OVER (PARTITION BY 1));
```

Output:

```
POLYGON ((0.5 1, 1 1, 1 0.5, 0.5 0.5, 0.5 1))
```

## ST_Union_Aggr

Introduction: Return the polygon union of all polygons in A

Format: `ST_Union_Aggr (A:geometryColumn)`

SQL example:

```sql
WITH src_tbl AS (
    SELECT sedona.ST_GeomFromText('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS geom
    UNION
    SELECT sedona.ST_GeomFromText('POLYGON ((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))') AS geom
)
SELECT sedona.ST_AsText(unioned)
FROM src_tbl,
     TABLE(sedona.ST_Union_Aggr(src_tbl.geom) OVER (PARTITION BY 1));
```

Output:

```
POLYGON ((0 0, 0 1, 0.5 1, 0.5 1.5, 1.5 1.5, 1.5 0.5, 1 0.5, 1 0, 0 0))
```
