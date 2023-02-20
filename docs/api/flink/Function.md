## ST_3DDistance

Introduction: Return the 3-dimensional minimum cartesian distance between A and B

Format: `ST_3DDistance (A:geometry, B:geometry)`

Since: `v1.3.0`

Example:

```sql
SELECT ST_3DDistance(polygondf.countyshape, polygondf.countyshape)
FROM polygondf
```

## ST_AddPoint

Introduction: Return Linestring with additional point at the given index, if position is not available the point will be added at the end of line.

Format: `ST_AddPoint(geom: geometry, point: geometry, position: integer)`

Format: `ST_AddPoint(geom: geometry, point: geometry)`

Since: `v1.3.0`

Example:
```sql
SELECT ST_AddPoint(ST_GeomFromText("LINESTRING(0 0, 1 1, 1 0)"), ST_GeomFromText("Point(21 52)"), 1)

SELECT ST_AddPoint(ST_GeomFromText("Linestring(0 0, 1 1, 1 0)"), ST_GeomFromText("Point(21 52)"))
```

Output:
```
LINESTRING(0 0, 21 52, 1 1, 1 0)
LINESTRING(0 0, 1 1, 1 0, 21 52)
```

## ST_Area

Introduction: Return the area of A

Format: `ST_Area (A:geometry)`

Since: `v1.3.0`

Example:

```sql
SELECT ST_Area(polygondf.countyshape)
FROM polygondf
```

## ST_AsBinary

Introduction: Return the Well-Known Binary representation of a geometry

Format: `ST_AsBinary (A:geometry)`

Since: `v1.3.0`

Example:

```sql
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

```sql
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
```sql
SELECT ST_AsEWKT(polygondf.countyshape)
FROM polygondf
```

## ST_AsGeoJSON

Introduction: Return the [GeoJSON](https://geojson.org/) string representation of a geometry

Format: `ST_AsGeoJSON (A:geometry)`

Since: `v1.3.0`

Spark SQL example:
```sql
SELECT ST_AsGeoJSON(polygondf.countyshape)
FROM polygondf
```

## ST_AsGML

Introduction: Return the [GML](https://www.ogc.org/standards/gml) string representation of a geometry

Format: `ST_AsGML (A:geometry)`

Since: `v1.3.0`

Spark SQL example:
```sql
SELECT ST_AsGML(polygondf.countyshape)
FROM polygondf
```

## ST_AsKML

Introduction: Return the [KML](https://www.ogc.org/standards/kml) string representation of a geometry

Format: `ST_AsKML (A:geometry)`

Since: `v1.3.0`

Spark SQL example:
```sql
SELECT ST_AsKML(polygondf.countyshape)
FROM polygondf
```

## ST_AsText

Introduction: Return the Well-Known Text string representation of a geometry

Format: `ST_AsText (A:geometry)`

Since: `v1.3.0`

Example:

```sql
SELECT ST_AsText(polygondf.countyshape)
FROM polygondf
```

## ST_Azimuth

Introduction: Returns Azimuth for two given points in radians null otherwise.

Format: `ST_Azimuth(pointA: Point, pointB: Point)`

Since: `v1.3.0`

Example:

```sql
SELECT ST_Azimuth(ST_POINT(0.0, 25.0), ST_POINT(0.0, 0.0))
```

Output: `3.141592653589793`

## ST_Boundary

Introduction: Returns the closure of the combinatorial boundary of this Geometry.

Format: `ST_Boundary(geom: geometry)`

Since: `v1.3.0`

Example:
```sql
SELECT ST_Boundary(ST_GeomFromText('POLYGON ((1 1, 0 0, -1 1, 1 1))'))
```

Output: `LINEARRING (1 1, 0 0, -1 1, 1 1)`

## ST_Buffer

Introduction: Returns a geometry/geography that represents all points whose distance from this Geometry/geography is less than or equal to distance.

Format: `ST_Buffer (A:geometry, buffer: Double)`

Since: `v1.2.0`

Spark SQL example:
```sql
SELECT ST_Buffer(polygondf.countyshape, 1)
FROM polygondf
```

## ST_BuildArea

Introduction: Returns the areal geometry formed by the constituent linework of the input geometry.

Format: `ST_BuildArea (A:geometry)`

Since: `v1.2.1`

Example:

```sql
SELECT ST_BuildArea(ST_Collect(smallDf, bigDf)) AS geom
FROM smallDf, bigDf
```

Input: `MULTILINESTRING((0 0, 10 0, 10 10, 0 10, 0 0),(10 10, 20 10, 20 20, 10 20, 10 10))`

Output: `MULTIPOLYGON(((0 0,0 10,10 10,10 0,0 0)),((10 10,10 20,20 20,20 10,10 10)))`

## ST_ConcaveHull

Introduction: Return the Concave Hull of polgyon A, with alpha set to pctConvex[0, 1] in the Delaunay Triangulation method, the concave hull will not contain a hole unless allowHoles is set to true

Format: `ST_ConcaveHull (A:geometry, pctConvex:float)`

Format: `ST_ConcaveHull (A:geometry, pctConvex:float, allowHoles:Boolean)`

Since: `v1.4.0`

Example:

```sql
SELECT ST_ConcaveHull(polygondf.countyshape, pctConvex)`
FROM polygondf
```

Input: `Polygon ((0 0, 1 2, 2 2, 3 2, 5 0, 4 0, 3 1, 2 1, 1 0, 0 0))`

Output: `POLYGON ((1 2, 2 2, 3 2, 5 0, 4 0, 1 0, 0 0, 1 2))`

## ST_Distance

Introduction: Return the Euclidean distance between A and B

Format: `ST_Distance (A:geometry, B:geometry)`

Since: `v1.2.0`

Spark SQL example:
```sql
SELECT ST_Distance(polygondf.countyshape, polygondf.countyshape)
FROM polygondf
```

## ST_Envelope

Introduction: Return the envelop boundary of A

Format: `ST_Envelope (A:geometry)`

Since: `v1.3.0`

Example:

```sql
SELECT ST_Envelope(polygondf.countyshape)
FROM polygondf
```

## ST_ExteriorRing

Introduction: Returns a LINESTRING representing the exterior ring (shell) of a POLYGON. Returns NULL if the geometry is not a polygon.

Format: `ST_ExteriorRing(A:geometry)`

Since: `v1.2.1`

Examples:

```sql
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
```sql
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

```sql
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

```sql
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

## ST_GeometryN

Introduction: Return the 0-based Nth geometry if the geometry is a GEOMETRYCOLLECTION, (MULTI)POINT, (MULTI)LINESTRING, MULTICURVE or (MULTI)POLYGON. Otherwise, return null

Format: `ST_GeometryN(geom: geometry, n: Int)`

Since: `v1.3.0`

Example:
```sql
SELECT ST_GeometryN(ST_GeomFromText('MULTIPOINT((1 2), (3 4), (5 6), (8 9))'), 1)
```

Output: `POINT (3 4)`

## ST_InteriorRingN

Introduction: Returns the Nth interior linestring ring of the polygon geometry. Returns NULL if the geometry is not a polygon or the given N is out of range

Format: `ST_InteriorRingN(geom: geometry, n: Int)`

Since: `v1.3.0`

Example:
```sql
SELECT ST_InteriorRingN(ST_GeomFromText('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1), (1 3, 2 3, 2 4, 1 4, 1 3), (3 3, 4 3, 4 4, 3 4, 3 3))'), 0)
```

Output: `LINEARRING (1 1, 2 1, 2 2, 1 2, 1 1)`

## ST_IsClosed

Introduction: RETURNS true if the LINESTRING start and end point are the same.

Format: `ST_IsClosed(geom: geometry)`

Since: `v1.3.0`

Example:

```sql
SELECT ST_IsClosed(ST_GeomFromText('LINESTRING(0 0, 1 1, 1 0)'))
```

## ST_IsEmpty

Introduction: Test if a geometry is empty geometry

Format: `ST_IsEmpty (A:geometry)`

Since: `v1.2.1`

Spark SQL example:

```sql
SELECT ST_IsEmpty(polygondf.countyshape)
FROM polygondf
```

## ST_IsRing

Introduction: RETURN true if LINESTRING is ST_IsClosed and ST_IsSimple.

Format: `ST_IsRing(geom: geometry)`

Since: `v1.3.0`

Example:

```sql
SELECT ST_IsRing(ST_GeomFromText("LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)"))
```

Output: `true`

## ST_IsSimple

Introduction: Test if geometry's only self-intersections are at boundary points.

Format: `ST_IsSimple (A:geometry)`

Since: `v1.3.0`

Example:

```sql
SELECT ST_IsSimple(polygondf.countyshape)
FROM polygondf
```

## ST_IsValid

Introduction: Test if a geometry is well formed

Format: `ST_IsValid (A:geometry)`

Since: `v1.3.0`

Example:

```sql
SELECT ST_IsValid(polygondf.countyshape)
FROM polygondf
```

## ST_Length

Introduction: Return the perimeter of A

Format: ST_Length (A:geometry)

Since: `v1.3.0`

Example:

```sql
SELECT ST_Length(polygondf.countyshape)
FROM polygondf
```

## ST_LineFromMultiPoint

Introduction: Creates a LineString from a MultiPoint geometry.

Format: `ST_LineFromMultiPoint (A:geometry)`

Since: `v1.3.0`

Example:

```sql
SELECT ST_LineFromMultiPoint(df.geometry) AS geom
FROM df
```

Input: `MULTIPOINT((10 40), (40 30), (20 20), (30 10))`

Output: `LINESTRING (10 40, 40 30, 20 20, 30 10)`

## ST_Normalize

Introduction: Returns the input geometry in its normalized form.

Format

`ST_Normalize(geom: geometry)`

Since: `v1.3.0`

Example:

```sql
SELECT ST_AsEWKT(ST_Normalize(ST_GeomFromWKT('POLYGON((0 1, 1 1, 1 0, 0 0, 0 1))'))) AS geom
```

Result:

```
+-----------------------------------+
|geom                               |
+-----------------------------------+
|POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))|
+-----------------------------------+
```

## ST_NPoints

Introduction: Returns the number of points of the geometry

Since: `v1.3.0`

Format: `ST_NPoints (A:geometry)`

Example:
```sql
SELECT ST_NPoints(polygondf.countyshape)
FROM polygondf
```

## ST_NDims

Introduction: Returns the coordinate dimension of the geometry.

Format: `ST_NDims(geom: geometry)`

Since: `v1.3.1`

Spark SQL example with z co-rodinate:

```sql
SELECT ST_NDims(ST_GeomFromEWKT('POINT(1 1 2)'))
```

Output: `3`

Spark SQL example with x,y coordinate:

```sql
SELECT ST_NDims(ST_GeomFromText('POINT(1 1)'))
```

Output: `2`

## ST_NumGeometries

Introduction: Returns the number of Geometries. If geometry is a GEOMETRYCOLLECTION (or MULTI*) return the number of geometries, for single geometries will return 1.

Format: `ST_NumGeometries (A:geometry)`

Since: `v1.3.0`

Example:
```sql
SELECT ST_NumGeometries(df.geometry)
FROM df
```

## ST_NumInteriorRings

Introduction: Returns number of interior rings of polygon geometries.

Format: `ST_NumInteriorRings(geom: geometry)`

Since: `v1.3.0`

Example:
```sql
SELECT ST_NumInteriorRings(ST_GeomFromText('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))'))
```

Output: `1`

## ST_PointN

Introduction: Return the Nth point in a single linestring or circular linestring in the geometry. Negative values are counted backwards from the end of the LineString, so that -1 is the last point. Returns NULL if there is no linestring in the geometry.

Format: `ST_PointN(A:geometry, B:integer)`

Since: `v1.2.1`

Examples:

```sql
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

```sql
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

```sql
SELECT ST_Reverse(df.geometry) AS geom
FROM df
```

Input: `POLYGON ((-0.5 -0.5, -0.5 0.5, 0.5 0.5, 0.5 -0.5, -0.5 -0.5))`

Output: `POLYGON ((-0.5 -0.5, 0.5 -0.5, 0.5 0.5, -0.5 0.5, -0.5 -0.5))`

## ST_RemovePoint

Introduction: Return Linestring with removed point at given index, position can be omitted and then last one will be removed.

Format: `ST_RemovePoint(geom: geometry, position: integer)`

Format: `ST_RemovePoint(geom: geometry)`

Since: `v1.3.0`

Example:
```sql
SELECT ST_RemovePoint(ST_GeomFromText("LINESTRING(0 0, 1 1, 1 0)"), 1)
```

Output: `LINESTRING(0 0, 1 0)`

## ST_S2CellIDs

Introduction: Cover the geometry with Google S2 Cells, return the corresponding cell IDs with the given level.
The level indicates the [size of cells](https://s2geometry.io/resources/s2cell_statistics.html). With a bigger level,
the cells will be smaller, the coverage will be more accurate, but the result size will be exponentially increasing.

Format: `ST_S2CellIDs(geom: geometry, level: Int)`

Since: `v1.4.0`

Example:
```SQL
SELECT ST_S2CellIDs(ST_GeomFromText('LINESTRING(1 3 4, 5 6 7)'), 6)
```

Output:
```
[1159395429071192064, 1159958379024613376, 1160521328978034688, 1161084278931456000, 1170091478186196992, 1170654428139618304]
```

## ST_SetPoint

Introduction: Replace Nth point of linestring with given point. Index is 0-based. Negative index are counted backwards, e.g., -1 is last point.

Format: `ST_SetPoint (linestring: geometry, index: integer, point: geometry)`

Since: `v1.3.0`

Example:

```sql
SELECT ST_SetPoint(ST_GeomFromText('LINESTRING (0 0, 0 1, 1 1)'), 2, ST_GeomFromText('POINT (1 0)')) AS geom
```

Result:

```
+--------------------------------+
|                           geom |
+--------------------------------+
|     LINESTRING (0 0, 0 1, 1 0) |
+--------------------------------+
```

## ST_SetSRID

Introduction: Sets the spatial reference system identifier (SRID) of the geometry.

Format: `ST_SetSRID (A:geometry, srid: integer)`

Since: `v1.3.0`

Example:
```sql
SELECT ST_SetSRID(polygondf.countyshape, 3021)
FROM polygondf
```

## ST_SRID

Introduction: Return the spatial reference system identifier (SRID) of the geometry.

Format: `ST_SRID (A:geometry)`

Since: `v1.3.0`

Example:
```sql
SELECT ST_SRID(polygondf.countyshape)
FROM polygondf
```

## ST_Transform

Introduction:

Transform the Spatial Reference System / Coordinate Reference System of A, from SourceCRS to TargetCRS
For SourceCRS and TargetCRS, WKT format is also available since v1.3.1.

!!!note
    By default, this function uses lat/lon order. You can use ==ST_FlipCoordinates== to swap X and Y.

!!!note
    If ==ST_Transform== throws an Exception called "Bursa wolf parameters required", you need to disable the error notification in ST_Transform. You can append a boolean value at the end.

Format: `ST_Transform (A:geometry, SourceCRS:string, TargetCRS:string ,[Optional] DisableError)`

Since: `v1.2.0`

Spark SQL example (simple):
```sql
SELECT ST_Transform(polygondf.countyshape, 'epsg:4326','epsg:3857') 
FROM polygondf
```

Spark SQL example (with optional parameters):
```sql
SELECT ST_Transform(polygondf.countyshape, 'epsg:4326','epsg:3857', false)
FROM polygondf
```

!!!note
    The detailed EPSG information can be searched on [EPSG.io](https://epsg.io/).

## ST_X

Introduction: Returns X Coordinate of given Point, null otherwise.

Format: `ST_X(pointA: Point)`

Since: `v1.3.0`

Example:
```sql
SELECT ST_X(ST_POINT(0.0 25.0))
```

Output: `0.0`

## ST_XMax

Introduction: Returns the maximum X coordinate of a geometry

Format: `ST_XMax (A:geometry)`

Since: `v1.2.1`

Example:

```sql
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

```sql
SELECT ST_XMin(df.geometry) AS xmin
FROM df
```

Input: `POLYGON ((-1 -11, 0 10, 1 11, 2 12, -1 -11))`

Output: `-1`

## ST_Y

Introduction: Returns Y Coordinate of given Point, null otherwise.

Format: `ST_Y(pointA: Point)`

Since: `v1.3.0`

Example:
```sql
SELECT ST_Y(ST_POINT(0.0 25.0))
```

Output: `25.0`

## ST_YMax

Introduction: Return the minimum Y coordinate of A

Format: `ST_YMax (A:geometry)`

Since: `v1.2.1`

Spark SQL example:
```sql
SELECT ST_YMax(ST_GeomFromText('POLYGON((0 0 1, 1 1 1, 1 2 1, 1 1 1, 0 0 1))'))
```

Output : 2

## ST_YMin

Introduction: Return the minimum Y coordinate of A

Format: `ST_Y_Min (A:geometry)`

Since: `v1.2.1`

Spark SQL example:
```sql
SELECT ST_YMin(ST_GeomFromText('POLYGON((0 0 1, 1 1 1, 1 2 1, 1 1 1, 0 0 1))'))
```

Output : 0

## ST_Z

Introduction: Returns Z Coordinate of given Point, null otherwise.

Format: `ST_Z(pointA: Point)`

Since: `v1.3.0`

Example:
```sql
SELECT ST_Z(ST_POINT(0.0 25.0 11.0))
```

Output: `11.0`

## ST_ZMax

Introduction: Returns Z maxima of the given geometry or null if there is no Z coordinate.

Format: `ST_ZMax(geom: geometry)`

Since: `v1.3.1`

Spark SQL example:
```sql
SELECT ST_ZMax(ST_GeomFromText('POLYGON((0 0 1, 1 1 1, 1 2 1, 1 1 1, 0 0 1))'))
```

Output: `1.0`

## ST_ZMin

Introduction: Returns Z minima of the given geometry or null if there is no Z coordinate.

Format: `ST_ZMin(geom: geometry)`

Since: `v1.3.1`

Spark SQL example:
```sql
SELECT ST_ZMin(ST_GeomFromText('LINESTRING(1 3 4, 5 6 7)'))
```

Output: `4.0`

