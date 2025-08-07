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
## ST_Envelope

Introduction: Return the envelope boundary of A

Format: `ST_Envelope (A: Geography)`

`ST_Envelope (A: List[Geography])`

Since: `v1.8.0`

SQL Example

```sql
SELECT ST_Envelope(ST_GeomFromWKT('LINESTRING(0 0, 1 3)'))
```

Output:

```
POLYGON ((0 0, 0 3, 1 3, 1 0, 0 0))
```

SQL Example

```sql
SELECT ST_Envelope(ST_GeomFromWKT('LINESTRING(0 0, 1 3)'))
```

Output:

```
POLYGON ((0 0, 0 3, 1 3, 1 0, 0 0))
```

SQL Example

```sql
SELECT ST_Envelope(ST_GeomFromWKT('LINESTRING(0 0, 170)'))
```

Output:

```
POLYGON ((0 0, 0 3, 1 3, 1 0, 0 0))
```

## ST_Distance

Introduction: Return the geodesic distance of A using WGS84 spheroid. Unit is meter.

Geography must be in EPSG:4326 (WGS84) projection and must be in ==lon/lat== order.

Format: `ST_Distance(A: Geography)`

Since: `v1.8.0`

SQL Example

```sql
SELECT ST_Distance(ST_GeogFromWKT('POINT (-0.56 51.3168)'), ST_GeogFromWKT('POINT (-3.1883 55.9533)'))
```

Output:

```
544430.9411996207
```

SQL Example

Cross meridian
```sql
SELECT ST_Distance(ST_GeogFromWKT('POINT (-0.56 180)'), ST_GeogFromWKT('POINT (-3.1883 55.9533)'))
```

Output:

```
544430.9411996207
```

## ST_MaxDistance

Introduction: Calculates and returns the length value representing the maximum distance between any two points across the input geopgraphies.

Format: `ST_MaxDistance(geog1: Geography, geog2: Geography)`

Since: `v1.8.0`

SQL Example:

```sql
SELECT ST_MaxDistance(
        ST_GeogFromText("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"),
        ST_GeogFromText("POLYGON ((10 20, 30 30, 40 20, 30 10, 10 20))")
)
```

Output:

```
36.05551275463989
```


## ST_ClosestPoint

Introduction: Returns the 2-dimensional point on geog1 that is closest to geog2. This is the first point of the shortest line between the geographies. If using 3D geometries, the Z coordinates will be ignored. If you have a 3D Geometry, you may prefer to use ST_3DClosestPoint.
It will throw an exception indicates illegal argument if one of the params is an empty geometry.
// test with 3D
Format: `ST_ClosestPoint(g1: Geography, g2: Geography)`

Since: `v1.8.0`

SQL Example

```sql
SELECT ST_AsText( ST_ClosestPoint(g1, g2)) As ptwkt;
```

Input: `g1: POINT (160 40), g2: LINESTRING (10 30, 50 50, 30 110, 70 90, 180 140, 130 190)`

Output: `POINT(160 40)`

Input: `g1: LINESTRING (10 30, 50 50, 30 110, 70 90, 180 140, 130 190), g2: POINT (160 40)`

Output: `POINT(125.75342465753425 115.34246575342466)`

Input: `g1: 'POLYGON ((190 150, 20 10, 160 70, 190 150))', g2: ST_Buffer('POINT(80 160)', 30)`

Output: `POINT(131.59149149528952 101.89887534906197)`


## ST_MinimumClearanceLine

Introduction: This function returns a two-point LineString geometry representing the minimum clearance distance of the input geography. If the input geography does not have a defined minimum clearance, such as for single Points or coincident MultiPoints, an empty LineString geometry is returned instead.

Format: `ST_MinimumClearanceLine(geography: Geography)`

Since: `v1.8.0`

SQL Example:

```sql
SELECT ST_MinimumClearanceLine(
        ST_GeogFromWKT('POLYGON ((65 18, 62 16, 64.5 16, 62 14, 65 14, 65 18))')
)
```

Output:

```
LINESTRING (64.5 16, 65 16)
```

## ST_AsText

Introduction: Return the Extended Well-Known Binary representation of a geometry.
EWKB is an extended version of WKB which includes the SRID of the geometry.
The format originated in PostGIS but is supported by many GIS tools.
If the geometry is lacking SRID a WKB format is produced.
[See ST_SetSRID](#st_setsrid)
It will ignore the M coordinate if present.

Format: `ST_AsEWKB (A: Geometry)`

Since: `v1.1.1`

SQL Example

```sql
SELECT ST_AsText()
```

Output:

```
POINT
```
