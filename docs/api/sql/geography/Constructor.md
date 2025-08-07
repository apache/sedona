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

Introduction: Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown).

Format:

`ST_GeomFromWKT (Wkt: String)`

`ST_GeomFromWKT (Wkt: String, srid: Integer)`

Since: `v1.8.1`

SQL Example

```sql
SELECT ST_GeogFromWKT('LINESTRING (1 2, 3 4, 5 6)')
```

Output:

```
LINESTRING (1 2, 3 4, 5 6)
```

## ST_GeogFromText

Introduction: Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown). Alias of [ST_GeogFromWKT](#st_geogfromwkt)

Format:

`ST_GeogFromText (Wkt: String)`

`ST_GeogFromText (Wkt: String, srid: Integer)`

Since: `v1.8.1`

SQL Example

```sql
SELECT ST_GeogFromWKT('LINESTRING (1 2, 3 4, 5 6)')
```

Output:

```
LINESTRING (1 2, 3 4, 5 6)
```

## ST_GeogFromWKB

Introduction: Construct a Geometry from WKB string or Binary. This function also supports EWKB format.

Format:

`ST_GeogFromWKB (Wkb: String)`

`ST_GeogFromWKB (Wkb: Binary)`

Since: `v1.8.0`

SQL Example

```sql
SELECT ST_GeogFromWKB([1, 2, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, -124, -42, 0, -64, 0, 0, 0, 0, -128, -75,
                          -42, -65, 0, 0, 0, 96, -31, -17, -9, -65, 0, 0, 0, -128, 7, 93, -27, -65])
```

Output:

```
LINESTRING (-2.1047439575195317 -0.35482788085937506, -1.4960645437240603 -0.6676061153411864)```
