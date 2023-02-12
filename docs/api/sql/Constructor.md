## Read ESRI Shapefile
Introduction: Construct a DataFrame from a Shapefile

Since: `v1.0.0`

SparkSQL example:

```scala
var spatialRDD = new SpatialRDD[Geometry]
spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
var rawSpatialDf = Adapter.toDf(spatialRDD,sparkSession)
rawSpatialDf.createOrReplaceTempView("rawSpatialDf")
var spatialDf = sparkSession.sql("""
          | ST_GeomFromWKT(rddshape), _c1, _c2
          | FROM rawSpatialDf
        """.stripMargin)
spatialDf.show()
spatialDf.printSchema()
```

!!!note
	The file extensions of .shp, .shx, .dbf must be in lowercase. Assume you have a shape file called ==myShapefile==, the file structure should be like this:
	```
	- shapefile1
	- shapefile2
	- myshapefile
	- myshapefile.shp
	- myshapefile.shx
	- myshapefile.dbf
	- myshapefile...
	- ...
	```

!!!warning
	Please make sure you use ==ST_GeomFromWKT== to create Geometry type column otherwise that column cannot be used in SedonaSQL.

If the file you are reading contains non-ASCII characters you'll need to explicitly set the encoding
via `sedona.global.charset` system property before the call to `ShapefileReader.readToGeometryRDD`.

Example:

```scala
System.setProperty("sedona.global.charset", "utf8")
```

## ST_GeomFromGeoHash

Introduction: Create Geometry from geohash string and optional precision

Format: `ST_GeomFromGeoHash(geohash: string, precision: int)`

Since: `v1.1.1`

Spark SQL example:
```sql
SELECT ST_GeomFromGeoHash('s00twy01mt', 4) AS geom
```

result:

```
+--------------------------------------------------------------------------------------------------------------------+
|geom                                                                                                                |
+--------------------------------------------------------------------------------------------------------------------+
|POLYGON ((0.703125 0.87890625, 0.703125 1.0546875, 1.0546875 1.0546875, 1.0546875 0.87890625, 0.703125 0.87890625)) |
+--------------------------------------------------------------------------------------------------------------------+
```

## ST_GeomFromGeoJSON

Introduction: Construct a Geometry from GeoJson

Format: `ST_GeomFromGeoJSON (GeoJson:string)`

Since: `v1.0.0`

Spark SQL example:
```scala
var polygonJsonDf = sparkSession.read.format("csv").option("delimiter","\t").option("header","false").load(geoJsonGeomInputLocation)
polygonJsonDf.createOrReplaceTempView("polygontable")
polygonJsonDf.show()
var polygonDf = sparkSession.sql(
        """
          | SELECT ST_GeomFromGeoJSON(polygontable._c0) AS countyshape
          | FROM polygontable
        """.stripMargin)
polygonDf.show()
```

!!!warning
	The way that SedonaSQL reads GeoJSON is different from that in SparkSQL

## ST_GeomFromGML

Introduction: Construct a Geometry from GML.

Format:
`ST_GeomFromGML (gml:string)`

Since: `v1.3.0`

SQL example:
```sql
SELECT ST_GeomFromGML('<gml:LineString srsName="EPSG:4269"><gml:coordinates>-71.16028,42.258729 -71.160837,42.259112 -71.161143,42.25932</gml:coordinates></gml:LineString>') AS geometry
```

## ST_GeomFromKML

Introduction: Construct a Geometry from KML.

Format:
`ST_GeomFromKML (kml:string)`

Since: `v1.3.0`

SQL example:
```sql
SELECT ST_GeomFromKML('<LineString><coordinates>-71.1663,42.2614 -71.1667,42.2616</coordinates></LineString>') AS geometry
```

## ST_GeomFromText

Introduction: Construct a Geometry from Wkt. If srid is not set, it defaults to 0 (unknown). Alias of [ST_GeomFromWKT](#ST_GeomFromWKT)

Format:
`ST_GeomFromText (Wkt:string)`
`ST_GeomFromText (Wkt:string, srid:integer)`

Since: `v1.0.0`

The optional srid parameter was added in `v1.3.1`

Spark SQL example:
```sql
SELECT ST_GeomFromText('POINT(40.7128 -74.0060)') AS geometry
```

## ST_GeomFromWKB

Introduction: Construct a Geometry from WKB string or Binary

Format:
`ST_GeomFromWKB (Wkb:string)`
`ST_GeomFromWKB (Wkb:binary)`

Since: `v1.0.0`

Spark SQL example:
```sql
SELECT ST_GeomFromWKB(polygontable._c0) AS polygonshape
FROM polygontable
```

## ST_GeomFromWKT

Introduction: Construct a Geometry from Wkt. If srid is not set, it defaults to 0 (unknown).

Format:
`ST_GeomFromWKT (Wkt:string)`
`ST_GeomFromWKT (Wkt:string, srid:integer)`

Since: `v1.0.0`

The optional srid parameter was added in `v1.3.1`

Spark SQL example:
```sql
SELECT ST_GeomFromWKT(polygontable._c0) AS polygonshape
FROM polygontable
```

```sql
SELECT ST_GeomFromWKT('POINT(40.7128 -74.0060)') AS geometry
```

## ST_LineFromText

Introduction: Construct a Line from Wkt text

Format:
`ST_LineFromText (Wkt:string)`

Since: `v1.2.1`

Spark SQL example:
```sql
SELECT ST_LineFromText(linetable._c0) AS lineshape
FROM linetable
```

```sql
SELECT ST_LineFromText('Linestring(1 2, 3 4)') AS line
```

## ST_LineStringFromText

Introduction: Construct a LineString from Text, delimited by Delimiter

Format: `ST_LineStringFromText (Text:string, Delimiter:char)`

Since: `v1.0.0`

Spark SQL example:
```sql
SELECT ST_LineStringFromText(linestringtable._c0,',') AS linestringshape
FROM linestringtable
```

```sql
SELECT ST_LineStringFromText('-74.0428197,40.6867969,-74.0421975,40.6921336,-74.0508020,40.6912794', ',') AS linestringshape
```
## ST_MLineFromText

Introduction: Construct a MultiLineString from Wkt. If srid is not set, it defaults to 0 (unknown).

Format:
`ST_MLineFromText (Wkt:string)`
`ST_MLineFromText (Wkt:string, srid:integer)`

Since: `v1.3.1`

Spark SQL example:
```sql
SELECT ST_MLineFromText('MULTILINESTRING((1 2, 3 4), (4 5, 6 7))') AS multiLine;
SELECT ST_MLineFromText('MULTILINESTRING((1 2, 3 4), (4 5, 6 7))',4269) AS multiLine;
```

## ST_MPolyFromText

Introduction: Construct a MultiPolygon from Wkt. If srid is not set, it defaults to 0 (unknown).

Format:
`ST_MPolyFromText (Wkt:string)`
`ST_MPolyFromText (Wkt:string, srid:integer)`

Since: `v1.3.1`

Spark SQL example:
```sql
SELECT ST_MPolyFromText('MULTIPOLYGON(((-70.916 42.1002,-70.9468 42.0946,-70.9765 42.0872 )))') AS multiPolygon
SELECT ST_MPolyFromText('MULTIPOLYGON(((-70.916 42.1002,-70.9468 42.0946,-70.9765 42.0872 )))',4269) AS multiPolygon

```

## ST_Point

Introduction: Construct a Point from X and Y

Format: `ST_Point (X:decimal, Y:decimal)`

Since: `v1.0.0`

In `v1.4.0` an optional Z parameter was removed to be more consistent with other spatial SQL implementations.
If you are upgrading from an older version of Sedona - please use ST_PointZ to create 3D points.

Spark SQL example:
```sql
SELECT ST_Point(CAST(pointtable._c0 AS Decimal(24,20)), CAST(pointtable._c1 AS Decimal(24,20))) AS pointshape
FROM pointtable
```

## ST_PointZ

Introduction: Construct a Point from X, Y and Z and an optional srid. If srid is not set, it defaults to 0 (unknown).

Format: `ST_PointZ (X:decimal, Y:decimal, Z:decimal)`
Format: `ST_PointZ (X:decimal, Y:decimal, Z:decimal, srid:integer)`

Since: `v1.4.0`

Spark SQL example:
```sql
SELECT ST_PointZ(1.0, 2.0, 3.0) AS pointshape
```

## ST_PointFromText

Introduction: Construct a Point from Text, delimited by Delimiter

Format: `ST_PointFromText (Text:string, Delimiter:char)`

Since: `v1.0.0`

Spark SQL example:
```sql
SELECT ST_PointFromText(pointtable._c0,',') AS pointshape
FROM pointtable
```

```sql
SELECT ST_PointFromText('40.7128,-74.0060', ',') AS pointshape
```

## ST_PolygonFromEnvelope

Introduction: Construct a Polygon from MinX, MinY, MaxX, MaxY.

Format: `ST_PolygonFromEnvelope (MinX:decimal, MinY:decimal, MaxX:decimal, MaxY:decimal)`

Since: `v1.0.0`

Spark SQL example:
```sql
SELECT *
FROM pointdf
WHERE ST_Contains(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.pointshape)
```

## ST_PolygonFromText

Introduction: Construct a Polygon from Text, delimited by Delimiter. Path must be closed

Format: `ST_PolygonFromText (Text:string, Delimiter:char)`

Since: `v1.0.0`

Spark SQL example:
```sql
SELECT ST_PolygonFromText(polygontable._c0,',') AS polygonshape
FROM polygontable
```

```sql
SELECT ST_PolygonFromText('-74.0428197,40.6867969,-74.0421975,40.6921336,-74.0508020,40.6912794,-74.0428197,40.6867969', ',') AS polygonshape
```
