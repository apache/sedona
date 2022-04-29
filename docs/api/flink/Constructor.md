## ST_GeomFromWKT

Introduction: Construct a Geometry from Wkt

Format:
`ST_GeomFromWKT (Wkt:string)`

Since: `v1.2.0`

SQL example:
```SQL
SELECT ST_GeomFromWKT('POINT(40.7128 -74.0060)') AS geometry
```

## ST_GeomFromWKB

Introduction: Construct a Geometry from WKB string

Format:
`ST_GeomFromWKB (Wkb:string)`

Since: `v1.2.0`

SQL example:
```SQL
SELECT ST_GeomFromWKB(polygontable._c0) AS polygonshape
FROM polygontable
```

## ST_GeomFromGeoJSON

Introduction: Construct a Geometry from GeoJson

Format: `ST_GeomFromGeoJSON (GeoJson:string)`

Since: `v1.2.0`

SQL example:
```SQL
SELECT ST_GeomFromGeoJSON(polygontable._c0) AS polygonshape
FROM polygontable
```

## ST_PointFromText

Introduction: Construct a Point from Text, delimited by Delimiter

Format: `ST_PointFromText (Text:string, Delimiter:char)`

Since: `v1.2.0`

SQL example:
```SQL
SELECT ST_PointFromText('40.7128,-74.0060', ',') AS pointshape
```

## ST_PolygonFromText

Introduction: Construct a Polygon from Text, delimited by Delimiter. Path must be closed

Format: `ST_PolygonFromText (Text:string, Delimiter:char)`

Since: `v1.2.0`

SQL example:
```SQL
SELECT ST_PolygonFromText('-74.0428197,40.6867969,-74.0421975,40.6921336,-74.0508020,40.6912794,-74.0428197,40.6867969', ',') AS polygonshape
```

## ST_PolygonFromEnvelope

Introduction: Construct a Polygon from MinX, MinY, MaxX, MaxY.

Format: `ST_PolygonFromEnvelope (MinX:decimal, MinY:decimal, MaxX:decimal, MaxY:decimal)`

Since: `v1.2.0`

SQL example:
```SQL
SELECT *
FROM pointdf
WHERE ST_Contains(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.pointshape)
```

## ST_GeomFromGeoHash

Introduction: Create Geometry from geohash string and optional precision

Format: `ST_GeomFromGeoHash(geohash: string, precision: int)`

Since: `v1.2.1`

SQL example:
```SQL
SELECT ST_GeomFromGeoHash('s00twy01mt', 4) AS geom
```