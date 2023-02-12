## ST_GeomFromGeoHash

Introduction: Create Geometry from geohash string and optional precision

Format: `ST_GeomFromGeoHash(geohash: string, precision: int)`

Since: `v1.2.1`

SQL example:
```sql
SELECT ST_GeomFromGeoHash('s00twy01mt', 4) AS geom
```

## ST_GeomFromGeoJSON

Introduction: Construct a Geometry from GeoJson

Format: `ST_GeomFromGeoJSON (GeoJson:string)`

Since: `v1.2.0`

SQL example:
```sql
SELECT ST_GeomFromGeoJSON(polygontable._c0) AS polygonshape
FROM polygontable
```

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

Introduction: Construct a Geometry from Wkt. Alias of  [ST_GeomFromWKT](#ST_GeomFromWKT)

Format:
`ST_GeomFromText (Wkt:string)`

Since: `v1.2.1`

SQL example:
```sql
SELECT ST_GeomFromText('POINT(40.7128 -74.0060)') AS geometry
```

## ST_GeomFromWKB

Introduction: Construct a Geometry from WKB string or Binary

Format:
`ST_GeomFromWKB (Wkb:string)`
`ST_GeomFromWKB (Wkb:binary)`

Since: `v1.2.0`

SQL example:
```sql
SELECT ST_GeomFromWKB(polygontable._c0) AS polygonshape
FROM polygontable
```

Format:
`ST_GeomFromWKB (Wkb:bytes)`

Since: `v1.2.1`

SQL example:
```sql
SELECT ST_GeomFromWKB(polygontable._c0) AS polygonshape
FROM polygontable
```

## ST_GeomFromWKT

Introduction: Construct a Geometry from Wkt

Format:
`ST_GeomFromWKT (Wkt:string)`

Since: `v1.2.0`

SQL example:
```sql
SELECT ST_GeomFromWKT('POINT(40.7128 -74.0060)') AS geometry
```

## ST_LineFromText

Introduction: Construct a LineString from Text, delimited by Delimiter (Optional)

Format: `ST_LineFromText (Text:string, Delimiter:char)`

Since: `v1.2.1`

SQL example:
```sql
SELECT ST_LineFromText('Linestring(1 2, 3 4)') AS line
```

## ST_LineStringFromText

Introduction: Construct a LineString from Text, delimited by Delimiter (Optional). Alias of  [ST_LineFromText](#ST_LineFromText)

Format: `ST_LineStringFromText (Text:string, Delimiter:char)`

Since: `v1.2.1`

Spark SQL example:
```sql
SELECT ST_LineStringFromText('Linestring(1 2, 3 4)') AS line
```

## ST_MLineFromText

Introduction: Construct a MultiLineString from Text and Optional SRID

Format: `ST_MLineFromText (Text:string, Srid: int)`

Since: `1.3.1`

SQL example:
```sql
SELECT ST_MLineFromText('MULTILINESTRING((1 2, 3 4), (4 5, 6 7))') AS multiLine
SELECT ST_MLineFromText('MULTILINESTRING((1 2, 3 4), (4 5, 6 7))', 4269) AS multiLine
```

## ST_MPolyFromText

Introduction: Construct a MultiPolygon from Text and Optional SRID

Format: `ST_MPolyFromText (Text:string, Srid: int)`

Since: `1.3.1`

SQL example:
```sql
SELECT ST_MPolyFromText('MULTIPOLYGON(((-70.916 42.1002,-70.9468 42.0946,-70.9765 42.0872 )))') AS multiPolygon
SELECT ST_MPolyFromText('MULTIPOLYGON(((-70.916 42.1002,-70.9468 42.0946,-70.9765 42.0872 )))', 4269) AS multiPolygon
```

## ST_Point

Introduction: Construct a Point from X and Y

Format: `ST_Point (X:decimal, Y:decimal)`

Since: `v1.2.1`

SQL example:
```sql
SELECT ST_Point(x, y) AS pointshape
FROM pointtable
```

## ST_PointFromText

Introduction: Construct a Point from Text, delimited by Delimiter

Format: `ST_PointFromText (Text:string, Delimiter:char)`

Since: `v1.2.0`

SQL example:
```sql
SELECT ST_PointFromText('40.7128,-74.0060', ',') AS pointshape
```

## ST_PolygonFromEnvelope

Introduction: Construct a Polygon from MinX, MinY, MaxX, MaxY.

Format: `ST_PolygonFromEnvelope (MinX:decimal, MinY:decimal, MaxX:decimal, MaxY:decimal)`

Since: `v1.2.0`

SQL example:
```sql
SELECT *
FROM pointdf
WHERE ST_Contains(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.pointshape)
```

## ST_PolygonFromText

Introduction: Construct a Polygon from Text, delimited by Delimiter. Path must be closed

Format: `ST_PolygonFromText (Text:string, Delimiter:char)`

Since: `v1.2.0`

SQL example:
```sql
SELECT ST_PolygonFromText('-74.0428197,40.6867969,-74.0421975,40.6921336,-74.0508020,40.6912794,-74.0428197,40.6867969', ',') AS polygonshape
```
