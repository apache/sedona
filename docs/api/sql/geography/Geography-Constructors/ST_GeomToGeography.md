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

# ST_GeomToGeography

Introduction:

This function constructs a Geography object from a planar Geometry. This function is intended for geometries defined in a Geographic Coordinate Reference System (CRS), most commonly WGS84 (EPSG:4326), where coordinates are expressed in degrees and the longitude/latitude order

If the input Geometry is defined in a projected CRS (e.g., Web Mercator EPSG:3857, UTM zones), the conversion may succeed syntactically, but the resulting Geography will not be meaningful. This is because Geography interprets coordinates on the surface of a sphere, not a flat plane.

Sedona does not validate or enforce the SRID of the input Geometry. Whatever SRID is attached to the Geometry will simply be carried over to the Geography, even if it is inappropriate for spherical interpretation. It is the user’s responsibility to ensure the input Geometry uses a Geographic CRS.

Format:

`ST_GeomToGeography (geom: Geometry)`

Return type: `Geography`

Since: `v1.8.0`

SQL example:

```sql
SELECT ST_GeomToGeography(ST_GeomFromWKT('MULTIPOLYGON (((10 10, 70 10, 70 70, 10 70, 10 10), (20 20, 60 20, 60 60, 20 60, 20 20)), ((30 30, 50 30, 50 50, 30 50, 30 30), (36 36, 44 36, 44 44, 36 44, 36 36)))'))
```

Output:

```
MULTIPOLYGON (((10 10, 70 10, 70 70, 10 70, 10 10), (20 20, 60 20, 60 60, 20 60, 20 20)), ((30 30, 50 30, 50 50, 30 50, 30 30), (36 36, 44 36, 44 44, 36 44, 36 36)))
```
