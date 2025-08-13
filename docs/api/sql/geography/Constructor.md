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

## ST_GeomFromGeoHash

Introduction: Create Geography from geohash string and optional precision

Format: `ST_GeogFromGeoHash(geohash: String, precision: Integer)`

Since: `v1.8.0`

SQL Example

```sql
SELECT ST_GeogFromGeoHash('9q9j8ue2v71y5zzy0s4q', 16)
```

Output:

```
POLYGON ((-122.3061 37.554162, -122.3061 37.554162, -122.3061 37.554162, -122.3061 37.554162, -122.3061 37.554162))"
```

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

SQL Example

```sql
SELECT ST_GeogFromWKT('LINESTRING (1 2, 3 4, 5 6)', 4326)
```

Output:

```
SRID=4326; LINESTRING (1 2, 3 4, 5 6)
```

## ST_GeogFromEWKT

Introduction: Construct a Geography from OGC Extended WKT.

Format:
`ST_GeogFromEWKT (EWkt: String)`

Since: `v1.8.0`

SQL example:

```sql
SELECT ST_GeogFromEWKT('SRID=4326; LINESTRING (0 0, 3 3, 4 4)')
```

Output:

```
SRID=4326; LINESTRING (0 0, 3 3, 4 4)
```
