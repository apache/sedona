## ST_3DDistance

Introduction: Return the 3-dimensional minimum cartesian distance between A and B

Format: `ST_3DDistance (A:geometry, B:geometry)`

Since: `v1.3.0`

Example:

```SQL
SELECT ST_3DDistance(polygondf.countyshape, polygondf.countyshape)
FROM polygondf
```

## ST_Area

Introduction: Return the area of A

Format: `ST_Area (A:geometry)`

Since: `v1.3.0`

Example:

```SQL
SELECT ST_Area(polygondf.countyshape)
FROM polygondf
```

## ST_AsBinary

Introduction: Return the Well-Known Binary representation of a geometry

Format: `ST_AsBinary (A:geometry)`

Since: `v1.3.0`

Example:

```SQL
SELECT ST_AsBinary(polygondf.countyshape)
FROM polygondf
```

## ST_AsEWKB

Introduction: Return the Extended Well-Known Binary representation of a geometry.
EWKB is an extended version of WKB which includes the SRID of the geometry.
The format originated in PostGIS but is supported by many GIS tools.
If the geometry is lacking SRID a WKB format is produced.

Format: `ST_AsEWKB (A:geometry)`

Since: `v1.3.0`

Example:

```SQL
SELECT ST_AsEWKB(polygondf.countyshape)
FROM polygondf
```

## ST_AsEWKT

Introduction: Return the Extended Well-Known Text representation of a geometry.
EWKT is an extended version of WKT which includes the SRID of the geometry.
The format originated in PostGIS but is supported by many GIS tools.
If the geometry is lacking SRID a WKT format is produced.
[See ST_SetSRID](#ST_SetSRID)

Format: `ST_AsEWKT (A:geometry)`

Since: `v1.2.1`

Spark SQL example:
```SQL
SELECT ST_AsEWKT(polygondf.countyshape)
FROM polygondf
```

## ST_AsGeoJSON

Introduction: Return the [GeoJSON](https://geojson.org/) string representation of a geometry

Format: `ST_AsGeoJSON (A:geometry)`

Since: `v1.3.0`

Spark SQL example:
```SQL
SELECT ST_AsGeoJSON(polygondf.countyshape)
FROM polygondf
```

## ST_AsText

Introduction: Return the Well-Known Text string representation of a geometry

Format: `ST_AsText (A:geometry)`

Since: `v1.3.0`

Example:

```SQL
SELECT ST_AsText(polygondf.countyshape)
FROM polygondf
```

## ST_Azimuth

Introduction: Returns Azimuth for two given points in radians null otherwise.

Format: `ST_Azimuth(pointA: Point, pointB: Point)`

Since: `v1.3.0`

Example:

```SQL
SELECT ST_Azimuth(ST_POINT(0.0, 25.0), ST_POINT(0.0, 0.0))
```

Output: `3.141592653589793`

## ST_Buffer

Introduction: Returns a geometry/geography that represents all points whose distance from this Geometry/geography is less than or equal to distance.

Format: `ST_Buffer (A:geometry, buffer: Double)`

Since: `v1.2.0`

Spark SQL example:
```SQL
SELECT ST_Buffer(polygondf.countyshape, 1)
FROM polygondf
```

## ST_BuildArea

Introduction: Returns the areal geometry formed by the constituent linework of the input geometry.

Format: `ST_BuildArea (A:geometry)`

Since: `v1.2.1`

Example:

```SQL
SELECT ST_BuildArea(ST_Collect(smallDf, bigDf)) AS geom
FROM smallDf, bigDf
```

Input: `MULTILINESTRING((0 0, 10 0, 10 10, 0 10, 0 0),(10 10, 20 10, 20 20, 10 20, 10 10))`

Output: `MULTIPOLYGON(((0 0,0 10,10 10,10 0,0 0)),((10 10,10 20,20 20,20 10,10 10)))`

## ST_Distance

Introduction: Return the Euclidean distance between A and B

Format: `ST_Distance (A:geometry, B:geometry)`

Since: `v1.2.0`

Spark SQL example:
```SQL
SELECT ST_Distance(polygondf.countyshape, polygondf.countyshape)
FROM polygondf
```

## ST_ExteriorRing

Introduction: Returns a LINESTRING representing the exterior ring (shell) of a POLYGON. Returns NULL if the geometry is not a polygon.

Format: `ST_ExteriorRing(A:geometry)`

Since: `v1.2.1`

Examples:

```SQL
SELECT ST_ExteriorRing(df.geometry)
FROM df
```

Input: `POLYGON ((0 0, 1 1, 2 1, 0 1, 1 -1, 0 0))`

Output: `LINESTRING (0 0, 1 1, 2 1, 0 1, 1 -1, 0 0)`

## ST_FlipCoordinates

Introduction: Returns a version of the given geometry with X and Y axis flipped.

Format: `ST_FlipCoordinates(A:geometry)`

Since: `v1.2.0`

Spark SQL example:
```SQL
SELECT ST_FlipCoordinates(df.geometry)
FROM df
```

Input: `POINT (1 2)`

Output: `POINT (2 1)`

## ST_Force_2D

Introduction: Forces the geometries into a "2-dimensional mode" so that all output representations will only have the X and Y coordinates

Format: `ST_Force_2D (A:geometry)`

Since: `v1.2.1`

Example:

```SQL
SELECT ST_Force_2D(df.geometry) AS geom
FROM df
```

Input: `POLYGON((0 0 2,0 5 2,5 0 2,0 0 2),(1 1 2,3 1 2,1 3 2,1 1 2))`

Output: `POLYGON((0 0,0 5,5 0,0 0),(1 1,3 1,1 3,1 1))`

## ST_GeoHash

Introduction: Returns GeoHash of the geometry with given precision

Format: `ST_GeoHash(geom: geometry, precision: int)`

Since: `v1.2.0`

Example: 

Query:

```SQL
SELECT ST_GeoHash(ST_GeomFromText('POINT(21.427834 52.042576573)'), 5) AS geohash
```

Result:

```
+-----------------------------+
|geohash                      |
+-----------------------------+
|u3r0p                        |
+-----------------------------+
```

## ST_IsClosed

Introduction: RETURNS true if the LINESTRING start and end point are the same.

Format: `ST_IsClosed(geom: geometry)`

Since: `v1.3.0`

Example:

```SQL
SELECT ST_IsClosed(ST_GeomFromText('LINESTRING(0 0, 1 1, 1 0)'))
```

## ST_IsEmpty

Introduction: Test if a geometry is empty geometry

Format: `ST_IsEmpty (A:geometry)`

Since: `v1.2.1`

Spark SQL example:

```SQL
SELECT ST_IsEmpty(polygondf.countyshape)
FROM polygondf
```

## ST_IsRing

Introduction: RETURN true if LINESTRING is ST_IsClosed and ST_IsSimple.

Format: `ST_IsRing(geom: geometry)`

Since: `v1.3.0`

Example:

```SQL
SELECT ST_IsRing(ST_GeomFromText("LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)"))
```

Output: `true`

## ST_IsSimple

Introduction: Test if geometry's only self-intersections are at boundary points.

Format: `ST_IsSimple (A:geometry)`

Since: `v1.3.0`

Example:

```SQL
SELECT ST_IsSimple(polygondf.countyshape)
FROM polygondf
```

## ST_IsValid

Introduction: Test if a geometry is well formed

Format: `ST_IsValid (A:geometry)`

Since: `v1.3.0`

Example:

```SQL
SELECT ST_IsValid(polygondf.countyshape)
FROM polygondf
```

## ST_Length

Introduction: Return the perimeter of A

Format: ST_Length (A:geometry)

Since: `v1.3.0`

Example:

```SQL
SELECT ST_Length(polygondf.countyshape)
FROM polygondf
```

## ST_PointN

Introduction: Return the Nth point in a single linestring or circular linestring in the geometry. Negative values are counted backwards from the end of the LineString, so that -1 is the last point. Returns NULL if there is no linestring in the geometry.

Format: `ST_PointN(A:geometry, B:integer)`

Since: `v1.2.1`

Examples:

```SQL
SELECT ST_PointN(df.geometry, 2)
FROM df
```

Input: `LINESTRING(0 0, 1 2, 2 4, 3 6), 2`

Output: `POINT (1 2)`

Input: `LINESTRING(0 0, 1 2, 2 4, 3 6), -2`

Output: `POINT (2 4)`

Input: `CIRCULARSTRING(1 1, 1 2, 2 4, 3 6, 1 2, 1 1), -1`

Output: `POINT (1 1)`

## ST_PointOnSurface

Introduction: Returns a POINT guaranteed to lie on the surface.

Format: `ST_PointOnSurface(A:geometry)`

Since: `v1.2.1`

Examples:

```SQL
SELECT ST_PointOnSurface(df.geometry)
FROM df
```

1.  Input: `POINT (0 5)`

    Output: `POINT (0 5)`

2.  Input: `LINESTRING(0 5, 0 10)`

    Output: `POINT (0 5)`

3.  Input: `POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))`

    Output: `POINT (2.5 2.5)`

4.  Input: `LINESTRING(0 5 1, 0 0 1, 0 10 2)`

    Output: `POINT Z(0 0 1)`

## ST_Reverse

Introduction: Return the geometry with vertex order reversed

Format: `ST_Reverse (A:geometry)`

Since: `v1.2.1`

Example:

```SQL
SELECT ST_Reverse(df.geometry) AS geom
FROM df
```

Input: `POLYGON ((-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5))`

Output: `POLYGON ((-0.5 -0.5, 0.5 -0.5, 0.5 0.5, -0.5 0.5, -0.5 -0.5))`

## ST_SetSRID

Introduction: Sets the spatial refence system identifier (SRID) of the geometry.

Format: `ST_SetSRID (A:geometry, srid: integer)`

Since: `v1.3.0`

Example:
```SQL
SELECT ST_SetSRID(polygondf.countyshape, 3021)
FROM polygondf
```

## ST_SRID

Introduction: Return the spatial refence system identifier (SRID) of the geometry.

Format: `ST_SRID (A:geometry)`

Since: `v1.3.0`

Example:
```SQL
SELECT ST_SRID(polygondf.countyshape)
FROM polygondf
```

## ST_Transform

Introduction:

Transform the Spatial Reference System / Coordinate Reference System of A, from SourceCRS to TargetCRS

!!!note
By default, this function uses lat/lon order. You can use ==ST_FlipCoordinates== to swap X and Y.

!!!note
If ==ST_Transform== throws an Exception called "Bursa wolf parameters required", you need to disable the error notification in ST_Transform. You can append a boolean value at the end.

Format: `ST_Transform (A:geometry, SourceCRS:string, TargetCRS:string ,[Optional] DisableError)`

Since: `v1.2.0`

Spark SQL example (simple):
```SQL
SELECT ST_Transform(polygondf.countyshape, 'epsg:4326','epsg:3857') 
FROM polygondf
```

Spark SQL example (with optional parameters):
```SQL
SELECT ST_Transform(polygondf.countyshape, 'epsg:4326','epsg:3857', false)
FROM polygondf
```

!!!note
The detailed EPSG information can be searched on [EPSG.io](https://epsg.io/).

## ST_XMax

Introduction: Returns the maximum X coordinate of a geometry

Format: `ST_XMax (A:geometry)`

Since: `v1.2.1`

Example:

```SQL
SELECT ST_XMax(df.geometry) AS xmax
FROM df
```

Input: `POLYGON ((-1 -11, 0 10, 1 11, 2 12, -1 -11))`

Output: `2`

## ST_XMin

Introduction: Returns the minimum X coordinate of a geometry

Format: `ST_XMin (A:geometry)`

Since: `v1.2.1`

Example:

```SQL
SELECT ST_XMin(df.geometry) AS xmin
FROM df
```

Input: `POLYGON ((-1 -11, 0 10, 1 11, 2 12, -1 -11))`

Output: `-1`

## ST_YMax

Introduction: Return the minimum Y coordinate of A

Format: `ST_YMax (A:geometry)`

Since: `v1.2.1`

Spark SQL example:
```SQL
SELECT ST_YMax(ST_GeomFromText('POLYGON((0 0 1, 1 1 1, 1 2 1, 1 1 1, 0 0 1))'))
```

Output : 2

## ST_YMin

Introduction: Return the minimum Y coordinate of A

Format: `ST_Y_Min (A:geometry)`

Since: `v1.2.1`

Spark SQL example:
```SQL
SELECT ST_YMin(ST_GeomFromText('POLYGON((0 0 1, 1 1 1, 1 2 1, 1 1 1, 0 0 1))'))
```

Output : 0
