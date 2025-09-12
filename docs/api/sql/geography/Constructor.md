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
SELECT ST_AsEWKT(ST_GeogFromEWKB([01 02 00 00 20 E6 10 00 00 02 00 00 00 00 00 00 00 84 D6 00 C0 00 00 00 00 80 B5 D6 BF 00 00 00 60 E1 EF F7 BF 00 00 00 80 07 5D E5 BF]))
```

Output:

```
SRID: 4326; LINESTRING (-2.1 -0.4, -1.5 -0.7)
```

## ST_GeogFromGeoHash

Introduction: Create Geography from geohash string and optional precision

Format:

`ST_GeogFromGeoHash(geohash: String, precision: Integer)`

Since: `v1.8.0`

SQL Example

```sql
SELECT ST_GeogFromGeoHash('9q9j8ue2v71y5zzy0s4q', 16)
```

Output:

```
POLYGON ((-122.3061 37.554162, -122.3061 37.554162, -122.3061 37.554162, -122.3061 37.554162, -122.3061 37.554162))"
```

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

SQL Example

```sql
SELECT ST_AsEWKT(ST_GeogFromWKT('LINESTRING (1 2, 3 4, 5 6)', 4326))
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
SELECT ST_AsEWKT(ST_GeogFromEWKT('SRID=4326; LINESTRING (0 0, 3 3, 4 4)'))
```

Output:

```
SRID=4326; LINESTRING (0 0, 3 3, 4 4)
```

## ST_GeogToGeometry

Introduction:

This function constructs a planar Geometry object from a Geography. While Sedona makes every effort to preserve the original spatial object, the conversion is not always exact because Geography and Geometry have different underlying models:

* Geography represents shapes on the Earth’s surface (spherical).
* Geometry represents shapes on a flat, Euclidean plane.

This difference can cause certain ambiguities during conversion. For example:

* A polygon in Geography always refers to the region on the Earth’s surface that the ring encloses. When converted to Geometry, however, it becomes unclear whether the polygon is intended to represent the “inside” or its complement (the “outside”) on the sphere.
* Long edges that cross the antimeridian or cover poles may also be represented differently once projected into planar space.

In practice, Sedona preserves the coordinates and ring orientation as closely as possible, but you should be aware that some topological properties (e.g., area, distance) may not match exactly after conversion.

Sedona does not validate or enforce the SRID of the input Geography. Whatever SRID is attached to the Geography will be carried over to the resulting Geometry, even if it is not appropriate for planar interpretation. It is the user’s responsibility to ensure that the Geography’s SRID is meaningful in the target Geometry context.

Format:

`ST_GeogToGeometry (geog: Geography)`

Since: `v1.8.0`

SQL example:

```sql
SELECT ST_GeogToGeometry(ST_GeogFromWKT('MULTILINESTRING ((90 90, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))', 4326))
```

Output:

```
MULTILINESTRING ((90 90, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))
```

## ST_GeomToGeography

Introduction:

This function constructs a Geography object from a planar Geometry. This function is intended for geometries defined in a Geographic Coordinate Reference System (CRS), most commonly WGS84 (EPSG:4326), where coordinates are expressed in degrees and the longitude/latitude order

If the input Geometry is defined in a projected CRS (e.g., Web Mercator EPSG:3857, UTM zones), the conversion may succeed syntactically, but the resulting Geography will not be meaningful. This is because Geography interprets coordinates on the surface of a sphere, not a flat plane.

Sedona does not validate or enforce the SRID of the input Geometry. Whatever SRID is attached to the Geometry will simply be carried over to the Geography, even if it is inappropriate for spherical interpretation. It is the user’s responsibility to ensure the input Geometry uses a Geographic CRS.

Format:

`ST_GeomToGeography (geom: Geometry)`

Since: `v1.8.0`

SQL example:

```sql
SELECT ST_GeomToGeography(ST_GeomFromWKT('MULTIPOLYGON (((10 10, 70 10, 70 70, 10 70, 10 10), (20 20, 60 20, 60 60, 20 60, 20 20)), ((30 30, 50 30, 50 50, 30 50, 30 30), (36 36, 44 36, 44 44, 36 44, 36 36)))'))
```

Output:

```
MULTIPOLYGON (((10 10, 70 10, 70 70, 10 70, 10 10), (20 20, 60 20, 60 60, 20 60, 20 20)), ((30 30, 50 30, 50 50, 30 50, 30 30), (36 36, 44 36, 44 44, 36 44, 36 36)))
```
