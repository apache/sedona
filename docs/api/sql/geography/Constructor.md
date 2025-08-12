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

## ST_GeogFromWKT

Introduction: Construct a Geography from WKT. If SRID is not set, it defaults to 0 (unknown).

!!!note  The output WKT string uses the FIXED precision model, but all computations are performed using JTS’s default 16-digit precision model. Users can specify a different precision model when converting to a string if needed.

Format:

`ST_GeogFromWKT (Wkt: String)`

`ST_GeogFromWKT (Wkt: String, srid: Integer)`

Since: `v1.8.0`

SQL Example

```sql
SELECT ST_GeogFromWKT('LINESTRING (1 2, 3 4, 5 6)')
```

Output:

```
LINESTRING (1 2, 3 4, 5 6)
```

SQL Example:

```sql
SELECT ST_GeogFromWKT('GEOMETRYCOLLECTION (POINT (50 50), LINESTRING (20 30, 40 60, 80 90), POLYGON ((35 15, 45 15, 40 25, 35 15), (30 10, 40 20, 30 20, 30 10)))')
```

Output:

```
GEOMETRYCOLLECTION (POINT (50 50), LINESTRING (20 30, 40 60, 80 90), POLYGON ((35 15, 45 15, 40 25, 35 15), (30 10, 40 20, 30 20, 30 10)))
```

## ST_GeogFromEWKT

Introduction: Construct a Geography from OGC Extended WKT.

Format:
`ST_GeogFromEWKT (EWkt: String)`

Since: `v1.8.0`

SQL example:

```sql
SELECT ST_GeogFromEWKT('SRID=4326;LINESTRING (0 0, 3 3, 4 4)')
```

Output:

```
LINESTRING (0 0, 3 3, 4 4)
```


## ST_GeogFromWKB

Introduction: Construct a Geography from WKB Binary.

Format:

`ST_GeogFromWKB (Wkb: Binary)`

Since: `v1.8.0`

SQL Example

```sql
SELECT ST_GeogFromWKB([01 02 00 00 00 02 00 00 00 00 00 00 00 84 d6 00 c0 00 00 00 00 80 b5 d6 bf 00 00 00 60 e1 ef f7 bf 00 00 00 80 07 5d e5 bf])
```

Output:

```
LINESTRING (-2.1 -0.4, -1.5 -0.7)
```

## ST_GeogFromEWKB

Introduction: Construct a Geography from EWKB Binary. This function is an alias of [ST_GeogFromWKB](#st_geogfromwkb).

Format:
`ST_GeogFromEWKB (EWkb: Binary)`

Since: `v1.8.0`

SQL Example

```sql
SELECT ST_GeogFromEWKB([01 02 00 00 00 02 00 00 00 00 00 00 00 84 D6 00 C0 00 00 00 00 80 B5 D6 BF 00 00 00 60 E1 EF F7 BF 00 00 00 80 07 5D E5 BF])
```

Output:

```
LINESTRING (-2.1 -0.4, -1.5 -0.7)
```
