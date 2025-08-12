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
