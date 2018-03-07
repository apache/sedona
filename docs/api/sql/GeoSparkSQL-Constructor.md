Note: UUIDs ensure the shape uniqueness of a geometry.
### ST_GeomFromWKT (Wkt:string, UUID1, UUID2, ...)

Introduction:

*Construct a Geometry from Wkt. Unlimited UUID strings can be appended.*

Since: v1.0.0

Spark SQL example:
```
select ST_GeomFromWKT(polygontable._c0) as polygonshape from polygontable
```

### ST_GeomFromGeoJSON (GeoJson:string, UUID1, UUID2, ...)

Introduction:

*Construct a Geometry from GeoJson. Unlimited UUID strings can be appended.*

Since: v1.0.0

Spark SQL example:
```
var polygonJsonDf = sparkSession.read.format("csv").option("delimiter","\t").option("header","false").load(geoJsonGeomInputLocation)
polygonJsonDf.createOrReplaceTempView("polygontable")
polygonJsonDf.show()
var polygonDf = sparkSession.sql("select ST_GeomFromGeoJSON(polygontable._c0) as countyshape from polygontable")
polygonDf.show()
```

Regarding how to read JSON files in Spark, read [the document from DataBricks](https://docs.databricks.com/spark/latest/data-sources/read-json.html)


### ST_Point (X:decimal, Y:decimal, UUID1, UUID2, ...)

Introduction:

*Construct a Point from X and Y. Unlimited UUID strings can be appended.*

Since: v1.0.0

Spark SQL example:
```
select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable
```


### ST_PointFromText (Text:string, Delimiter:char, UUID1, UUID2, ...)

Introduction:

*Construct a Point from Text, delimited by Delimiter. Unlimited UUID strings can be appended.*

Spark SQL example:
```
select ST_PointFromText(pointtable._c0,',') as pointshape from pointtable
```

### ST_PolygonFromText (Text:string, Delimiter:char, UUID1, UUID2, ...)

Introduction:

*Construct a Polygon from Text, delimited by Delimiter. Unlimited UUID strings can be appended.*

Since: v1.0.0

Spark SQL example:
```
select ST_PolygonFromText(polygontable._c0,',') as polygonshape from polygontable
```

### ST_LineStringFromText (Text:string, Delimiter:char, UUID1, UUID2, ...)

Introduction:

*Construct a LineString from Text, delimited by Delimiter. Unlimited UUID strings can be appended.*

Since: v1.0.0

Spark SQL example:
```
select ST_LineStringFromText(linestringtable._c0,',') as linestringshape from linestringtable
```

### ST_PolygonFromEnvelope (MinX:decimal, MinY:decimal, MaxX:decimal, MaxY:decimal, UUID1, UUID2, ...)

Introduction:

*Construct a Polygon from MinX, MinY, MaxX, MaxY. Unlimited UUID strings can be appended.*

Since: v1.0.0

Spark SQL example:
```
select * from pointdf where ST_Contains(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.pointshape)
```

### ST_Circle (A:Geometry, Radius:decimal)

Introduction:

*Construct a Circle from A with a Radius.*

Since: v1.0.0

Spark SQL example:

```
select ST_Circle(pointdf.pointshape, 1.0) from pointdf)
```

### Construct DataFrame from ESRI Shapefile
Introduction:

*Construct a DataFrame from a Shapefile*

Since: v1.0.0

SparkSQL example:

```
var spatialRDD = new SpatialRDD[Geometry]
spatialRDD.rawSpatialRDD = ShapefileReader.readToGeometryRDD(sparkSession.sparkContext, shapefileInputLocation)
var rawSpatialDf = Adapter.toDf(spatialRDD,sparkSession)
rawSpatialDf.createOrReplaceTempView("rawSpatialDf")
var spatialDf = sparkSession.sql("SELECT ST_GeomFromWKT(rddshape), _c1, _c2 FROM rawSpatialDf")
spatialDf.show()
spatialDf.printSchema()
```