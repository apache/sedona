## ST_GeomFromWKT

Introduction: Construct a Geometry from Wkt

Format:
`ST_GeomFromWKT (Wkt:string)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_GeomFromWKT(polygontable._c0) AS polygonshape
FROM polygontable
```

```SQL
SELECT ST_GeomFromWKT('POINT(40.7128 -74.0060)') AS geometry
```

## ST_GeomFromWKB

Introduction: Construct a Geometry from WKB string

Format:
`ST_GeomFromWKB (Wkb:string)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_GeomFromWKB(polygontable._c0) AS polygonshape
FROM polygontable
```

## ST_GeomFromGeoJSON

Introduction: Construct a Geometry from GeoJson

Format: `ST_GeomFromGeoJSON (GeoJson:string)`

Since: `v1.0.0`

Spark SQL example:
```Scala
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

## Read ESRI Shapefile
Introduction: Construct a DataFrame from a Shapefile

Since: `v1.0.0`

SparkSQL example:

```Scala
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

```Scala
System.setProperty("sedona.global.charset", "utf8")
```


## ST_Point

Introduction: Construct a Point from X and Y

Format: `ST_Point (X:decimal, Y:decimal)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_Point(CAST(pointtable._c0 AS Decimal(24,20)), CAST(pointtable._c1 AS Decimal(24,20))) AS pointshape
FROM pointtable
```


## ST_PointFromText

Introduction: Construct a Point from Text, delimited by Delimiter

Format: `ST_PointFromText (Text:string, Delimiter:char)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_PointFromText(pointtable._c0,',') AS pointshape
FROM pointtable
```

```SQL
SELECT ST_PointFromText('40.7128,-74.0060', ',') AS pointshape
```

## ST_PolygonFromText

Introduction: Construct a Polygon from Text, delimited by Delimiter. Path must be closed

Format: `ST_PolygonFromText (Text:string, Delimiter:char)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_PolygonFromText(polygontable._c0,',') AS polygonshape
FROM polygontable
```

```SQL
SELECT ST_PolygonFromText('-74.0428197,40.6867969,-74.0421975,40.6921336,-74.0508020,40.6912794,-74.0428197,40.6867969', ',') AS polygonshape
```

## ST_LineStringFromText

Introduction: Construct a LineString from Text, delimited by Delimiter

Format: `ST_LineStringFromText (Text:string, Delimiter:char)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_LineStringFromText(linestringtable._c0,',') AS linestringshape
FROM linestringtable
```

```SQL
SELECT ST_LineStringFromText('-74.0428197,40.6867969,-74.0421975,40.6921336,-74.0508020,40.6912794', ',') AS linestringshape
```

## ST_PolygonFromEnvelope

Introduction: Construct a Polygon from MinX, MinY, MaxX, MaxY.

Format: `ST_PolygonFromEnvelope (MinX:decimal, MinY:decimal, MaxX:decimal, MaxY:decimal)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT *
FROM pointdf
WHERE ST_Contains(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.pointshape)
```

## ST_GeomFromGeoHash

Introduction: Create Geometry from geohash string and optional precision

Format: `ST_GeomFromGeoHash(geohash: string, precision: int)`

Since: `v1.1.1`

Spark SQL example:
```SQL
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
