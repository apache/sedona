## ST_Distance

Introduction: Return the Euclidean distance between A and B

Format: `ST_Distance (A:geometry, B:geometry)`

Since: `v1.2.0`

Spark SQL example:
```SQL
SELECT ST_Distance(polygondf.countyshape, polygondf.countyshape)
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

## ST_Buffer

Introduction: Returns a geometry/geography that represents all points whose distance from this Geometry/geography is less than or equal to distance.

Format: `ST_Buffer (A:geometry, buffer: Double)`

Since: `v1.2.0`

Spark SQL example:
```SQL
SELECT ST_Buffer(polygondf.countyshape, 1)
FROM polygondf
```

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