!!! note
	UUIDs ensure the shape uniqueness of a geometry. It can be any strings. This is only needed when you want to convert an Spatial DataFrame to an Spatial RDD and let each geometry carry some non-spatial attributes (e.g., price, age, ...).
## ST_GeomFromWKT

Introduction: Construct a Geometry from Wkt. Unlimited UUID strings can be appended.

Format:
`ST_GeomFromWKT (Wkt:string, UUID1, UUID2, ...)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_GeomFromWKT(polygontable._c0) AS polygonshape
FROM polygontable
```

```SQL
SELECT ST_GeomFromWKT('POINT(40.7128,-74.0060)') AS geometry
```

## ST_GeomFromWKB

Introduction: Construct a Geometry from WKB string. Unlimited UUID strings can be appended.

Format:
`ST_GeomFromWKB (Wkb:string, UUID1, UUID2, ...)`

Since: `v1.2.0`

Spark SQL example:
```SQL
SELECT ST_GeomFromWKB(polygontable._c0) AS polygonshape
FROM polygontable
```

## ST_GeomFromGeoJSON

Introduction: Construct a Geometry from GeoJson. Unlimited UUID strings can be appended.

Format: `ST_GeomFromGeoJSON (GeoJson:string, UUID1, UUID2, ...)`

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
	The way that GeoSparkSQL reads GeoJSON is different from that in SparkSQL

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
	Please make sure you use ==ST_GeomFromWKT== to create Geometry type column otherwise that column cannot be used in GeoSparkSQL.

If the file you are reading contains non-ASCII characters you'll need to explicitly set the encoding
via `geospark.global.charset` system property before the call to `ShapefileReader.readToGeometryRDD`.

Example:

```Scala
System.setProperty("geospark.global.charset", "utf8")
```


## ST_Point

Introduction: Construct a Point from X and Y. Unlimited UUID strings can be appended.

Format: `ST_Point (X:decimal, Y:decimal, UUID1, UUID2, ...)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_Point(CAST(pointtable._c0 AS Decimal(24,20)), CAST(pointtable._c1 AS Decimal(24,20))) AS pointshape
FROM pointtable
```


## ST_PointFromText

Introduction: Construct a Point from Text, delimited by Delimiter. Unlimited UUID strings can be appended.

Format: `ST_PointFromText (Text:string, Delimiter:char, UUID1, UUID2, ...)`

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

Introduction: Construct a Polygon from Text, delimited by Delimiter. Path must be closed. Unlimited UUID strings can be appended.

Format: `ST_PolygonFromText (Text:string, Delimiter:char, UUID1, UUID2, ...)`

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

Introduction: Construct a LineString from Text, delimited by Delimiter. Unlimited UUID strings can be appended.

Format: `ST_LineStringFromText (Text:string, Delimiter:char, UUID1, UUID2, ...)`

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

Introduction: Construct a Polygon from MinX, MinY, MaxX, MaxY. Unlimited UUID strings can be appended.

Format: `ST_PolygonFromEnvelope (MinX:decimal, MinY:decimal, MaxX:decimal, MaxY:decimal, UUID1, UUID2, ...)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT *
FROM pointdf
WHERE ST_Contains(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.pointshape)
```

## ST_Circle

Introduction: Construct a Circle from A with a Radius.

Format: `ST_Circle (A:Geometry, Radius:decimal)`

Since: `v1.0.0` - `v1.1.3`

Spark SQL example:

```SQL
SELECT ST_Circle(pointdf.pointshape, 1.0)
FROM pointdf
```

!!!note
	GeoSpark doesn't control the radius's unit (degree or meter). It is same with the geometry. To change the geometry's unit, please transform the coordinate reference system. See [ST_Transform](GeoSparkSQL-Function.md#st_transform).
