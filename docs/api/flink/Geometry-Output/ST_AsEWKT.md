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

# ST_AsEWKT

Introduction: Return the Extended Well-Known Text representation of a geometry.
EWKT is an extended version of WKT which includes the SRID of the geometry.
The format originated in PostGIS but is supported by many GIS tools.
If the geometry is lacking SRID a WKT format is produced.
[See ST_SetSRID](../Spatial-Reference-System/ST_SetSRID.md)
It will support M coordinate if present since v1.5.0.

![ST_AsEWKT](../../../image/ST_AsEWKT/ST_AsEWKT.svg "ST_AsEWKT")

Format: `ST_AsEWKT (A: Geometry)`

Return type: `String`

Since: `v1.2.1`

Example:

```sql
SELECT ST_AsEWKT(ST_SetSrid(ST_GeomFromWKT('POLYGON((0 0,0 1,1 1,1 0,0 0))'), 4326))
```

Output:

```
SRID=4326;POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))
```

Example:

```sql
SELECT ST_AsEWKT(ST_MakePointM(1.0, 1.0, 1.0))
```

Output:

```
POINT M(1 1 1)
```

Example:

```sql
SELECT ST_AsEWKT(ST_MakePoint(1.0, 1.0, 1.0, 1.0))
```

Output:

```
POINT ZM(1 1 1 1)
```
