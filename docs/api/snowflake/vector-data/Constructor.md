!!!note
    Please always keep the schema name `SEDONA` (e.g., `SEDONA.ST_GeomFromWKT`) when you use Sedona functions to avoid conflicting with Snowflake's built-in functions.

## ST_GeomCollFromText

Introduction: Constructs a GeometryCollection from the WKT with the given SRID. If SRID is not provided then it defaults to 0. It returns `null` if the WKT is not a `GEOMETRYCOLLECTION`.

Format:

`ST_GeomCollFromText (Wkt: String)`

`ST_GeomCollFromText (Wkt: String, srid: Integer)`

SQL Example:

```sql
SELECT ST_GeomCollFromText('GEOMETRYCOLLECTION (POINT (50 50), LINESTRING (20 30, 40 60, 80 90), POLYGON ((30 10, 40 20, 30 20, 30 10), (35 15, 45 15, 40 25, 35 15)))')
```

Output:

```
GEOMETRYCOLLECTION (POINT (50 50), LINESTRING (20 30, 40 60, 80 90), POLYGON ((30 10, 40 20, 30 20, 30 10), (35 15, 45 15, 40 25, 35 15)))
```

## ST_GeomFromEWKB

Introduction: Construct a Geometry from EWKB string or Binary. This function is an alias of [ST_GeomFromWKB](#st_geomfromwkb).

Format:

`ST_GeomFromEWKB (Wkb: String)`

`ST_GeomFromEWKB (Wkb: Binary)`

SQL Example

```sql
SELECT ST_GeomFromEWKB([01 02 00 00 00 02 00 00 00 00 00 00 00 84 D6 00 C0 00 00 00 00 80 B5 D6 BF 00 00 00 60 E1 EF F7 BF 00 00 00 80 07 5D E5 BF])
```

Output:

```
LINESTRING (-2.1047439575195312 -0.354827880859375, -1.49606454372406 -0.6676061153411865)
```

SQL Example

```sql
SELECT ST_asEWKT(ST_GeomFromEWKB('01010000a0e6100000000000000000f03f000000000000f03f000000000000f03f'))
```

Output:

```
SRID=4326;POINT Z(1 1 1)
```

## ST_GeomFromEWKT

Introduction: Construct a Geometry from OGC Extended WKT

Format:
`ST_GeomFromEWKT (EWkt:string)`

SQL example:

```sql
SELECT ST_AsText(ST_GeomFromEWKT('SRID=4269;POINT(40.7128 -74.0060)'))
```

Output:

```
POINT(40.7128 -74.006)
```

## ST_GeomFromGML

Introduction: Construct a Geometry from GML.

!!!note
    This function only supports GML1 and GML2. GML3 is not supported.

Format:
`ST_GeomFromGML (gml:string)`

SQL example:

```sql
SELECT ST_GeomFromGML('
    <gml:LineString srsName="EPSG:4269">
    	<gml:coordinates>
        	-71.16028,42.258729
        	-71.160837,42.259112
        	-71.161143,42.25932
    	</gml:coordinates>
    </gml:LineString>
')
```

Output:

```
LINESTRING (-71.16028 42.258729, -71.160837 42.259112, -71.161143 42.25932)
```

## ST_GeomFromGeoHash

Introduction: Create Geometry from geohash string and optional precision

Format: `ST_GeomFromGeoHash(geohash: string, precision: int)`

SQL example:

```sql
SELECT ST_GeomFromGeoHash('s00twy01mt', 4)
```

Output:

```
POLYGON ((0.703125 0.87890625, 0.703125 1.0546875, 1.0546875 1.0546875, 1.0546875 0.87890625, 0.703125 0.87890625))
```

## ST_GeomFromGeoJSON

Introduction: Construct a Geometry from GeoJson

Format: `ST_GeomFromGeoJSON (GeoJson:string)`

SQL example:

```sql
SELECT ST_GeomFromGeoJSON('{
   "type":"Feature",
   "properties":{
      "STATEFP":"01",
      "COUNTYFP":"077",
      "TRACTCE":"011501",
      "BLKGRPCE":"5",
      "AFFGEOID":"1500000US010770115015",
      "GEOID":"010770115015",
      "NAME":"5",
      "LSAD":"BG",
      "ALAND":6844991,
      "AWATER":32636
   },
   "geometry":{
      "type":"Polygon",
      "coordinates":[
         [
            [-87.621765, 34.873444],
            [-87.617535, 34.873369],
            [-87.62119, 34.85053],
            [-87.62144, 34.865379],
            [-87.621765, 34.873444]
         ]
      ]
   }
}')
```

Output:

```
POLYGON ((-87.621765 34.873444, -87.617535 34.873369, -87.62119 34.85053, -87.62144 34.865379, -87.621765 34.873444))
```

SQL example:

```sql
SELECT ST_GeomFromGeoJSON('{
   "type":"Polygon",
   "coordinates":[
	  [
	 	 [-87.621765, 34.873444],
 		 [-87.617535, 34.873369],
		 [-87.62119, 34.85053],
		 [-87.62144, 34.865379],
		 [-87.621765, 34.873444]
	  ]
   ]
}')
```

Output:

```
POLYGON ((-87.621765 34.873444, -87.617535 34.873369, -87.62119 34.85053, -87.62144 34.865379, -87.621765 34.873444))
```

## ST_GeomFromKML

Introduction: Construct a Geometry from KML.

Format:
`ST_GeomFromKML (kml:string)`

SQL example:

```sql
SELECT ST_GeomFromKML('
	<LineString>
		<coordinates>
			-71.1663,42.2614
			-71.1667,42.2616
		</coordinates>
	</LineString>
')
```

Output:

```
LINESTRING (-71.1663 42.2614, -71.1667 42.2616)
```

## ST_GeomFromText

Introduction: Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown). Alias of [ST_GeomFromWKT](#st_geomfromwkt)

Format:
`ST_GeomFromText (Wkt:string)`
`ST_GeomFromText (Wkt:string, srid:integer)`

SQL example:

```sql
SELECT ST_GeomFromText('POINT(40.7128 -74.0060)')
```

Output:

```
POINT(40.7128 -74.006)
```

## ST_GeomFromWKB

Introduction: Construct a Geometry from WKB string or Binary. This function also supports EWKB format.

Format:
`ST_GeomFromWKB (Wkb:string)`
`ST_GeomFromWKB (Wkb:binary)`

SQL example:

```sql
SELECT ST_GeomFromWKB([01 02 00 00 00 02 00 00 00 00 00 00 00 84 D6 00 C0 00 00 00 00 80 B5 D6 BF 00 00 00 60 E1 EF F7 BF 00 00 00 80 07 5D E5 BF])
```

Output:

```
LINESTRING (-2.1047439575195312 -0.354827880859375, -1.49606454372406 -0.6676061153411865)
```

SQL example:

```sql
SELECT ST_asEWKT(ST_GeomFromWKB('01010000a0e6100000000000000000f03f000000000000f03f000000000000f03f'))
```

Output:

```
SRID=4326;POINT Z(1 1 1)
```

## ST_GeomFromWKT

Introduction: Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown).

Format:
`ST_GeomFromWKT (Wkt:string)`
`ST_GeomFromWKT (Wkt:string, srid:integer)`

The optional srid parameter was added in `v1.3.1`

SQL example:

```sql
SELECT ST_GeomFromWKT('POINT(40.7128 -74.0060)')
```

Output:

```
POINT(40.7128 -74.006)
```

## ST_GeometryFromText

Introduction: Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown). Alias of [ST_GeomFromWKT](#st_geomfromwkt)

Format:

`ST_GeometryFromText (Wkt: String)`

`ST_GeometryFromText (Wkt: String, srid: Integer)`

SQL Example

```sql
SELECT ST_GeometryFromText('POINT(40.7128 -74.0060)')
```

Output:

```
POINT(40.7128 -74.006)
```

## ST_LineFromText

Introduction: Construct a Line from Wkt text

Format:
`ST_LineFromText (Wkt:string)`

SQL example:

```sql
SELECT ST_LineFromText('LINESTRING(1 2,3 4)')
```

Output:

```
LINESTRING (1 2, 3 4)
```

## ST_LineFromWKB

Introduction: Construct a LineString geometry from WKB string or Binary and an optional SRID. This function also supports EWKB format.

!!!note
    Returns null if geometry is not of type LineString.

Format:

`ST_LineFromWKB (Wkb: String)`

`ST_LineFromWKB (Wkb: Binary)`

`ST_LineFromWKB (Wkb: String, srid: Integer)`

`ST_LineFromWKB (Wkb: Binary, srid: Integer)`

Example:

```sql
SELECT ST_LineFromWKB([01 02 00 00 00 02 00 00 00 00 00 00 00 84 D6 00 C0 00 00 00 00 80 B5 D6 BF 00 00 00 60 E1 EF F7 BF 00 00 00 80 07 5D E5 BF])
```

Output:

```
LINESTRING (-2.1047439575195312 -0.354827880859375, -1.49606454372406 -0.6676061153411865)
```

## ST_LineStringFromText

Introduction: Construct a LineString from Text, delimited by Delimiter

Format: `ST_LineStringFromText (Text:string, Delimiter:char)`

SQL example:

```sql
SELECT ST_LineStringFromText('-74.0428197,40.6867969,-74.0421975,40.6921336,-74.0508020,40.6912794', ',')
```

Output:

```
LINESTRING (-74.0428197 40.6867969, -74.0421975 40.6921336, -74.050802 40.6912794)
```

## ST_LinestringFromWKB

Introduction: Construct a LineString geometry from WKB string or Binary and an optional SRID. This function also supports EWKB format and it is an alias of [ST_LineFromWKB](#st_linefromwkb).

!!!Note
    Returns null if geometry is not of type LineString.

Format:

`ST_LinestringFromWKB (Wkb: String)`

`ST_LinestringFromWKB (Wkb: Binary)`

`ST_LinestringFromWKB (Wkb: String, srid: Integer)`

`ST_LinestringFromWKB (Wkb: Binary, srid: Integer)`

Example:

```sql
SELECT ST_LinestringFromWKB([01 02 00 00 00 02 00 00 00 00 00 00 00 84 D6 00 C0 00 00 00 00 80 B5 D6 BF 00 00 00 60 E1 EF F7 BF 00 00 00 80 07 5D E5 BF])
```

Output:

```
LINESTRING (-2.1047439575195312 -0.354827880859375, -1.49606454372406 -0.6676061153411865)
```

## ST_MLineFromText

Introduction: Construct a MultiLineString from Wkt. If srid is not set, it defaults to 0 (unknown).

Format:
`ST_MLineFromText (Wkt:string)`
`ST_MLineFromText (Wkt:string, srid:integer)`

SQL example:

```sql
SELECT ST_MLineFromText('MULTILINESTRING((1 2, 3 4), (4 5, 6 7))')
```

Output:

```
MULTILINESTRING ((1 2, 3 4), (4 5, 6 7))
```

## ST_MPointFromText

Introduction: Constructs a MultiPoint from the WKT with the given SRID. If SRID is not provided then it defaults to 0. It returns `null` if the WKT is not a `MULTIPOINT`.

Format:

`ST_MPointFromText (Wkt: String)`

`ST_MPointFromText (Wkt: String, srid: Integer)`

SQL Example:

```sql
SELECT ST_MPointFromText('MULTIPOINT ((10 10), (20 20), (30 30))')
```

Output:

```
MULTIPOINT ((10 10), (20 20), (30 30))
```

## ST_MPolyFromText

Introduction: Construct a MultiPolygon from Wkt. If srid is not set, it defaults to 0 (unknown).

Format:
`ST_MPolyFromText (Wkt:string)`
`ST_MPolyFromText (Wkt:string, srid:integer)`

SQL example:

```sql
SELECT ST_MPolyFromText('MULTIPOLYGON(((0 0 1,20 0 1,20 20 1,0 20 1,0 0 1),(5 5 3,5 7 3,7 7 3,7 5 3,5 5 3)))')
```

Output:

```
MULTIPOLYGON (((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 5 7, 7 7, 7 5, 5 5)))
```

## ST_MakePoint

Introduction: Creates a 2D, 3D Z or 4D ZM Point geometry. Use ST_MakePointM to make points with XYM coordinates. Z and M values are optional.

Format: `ST_MakePoint (X:decimal, Y:decimal, Z:decimal, M:decimal)`

Example:

```sql
SELECT ST_AsText(ST_MakePoint(1.2345, 2.3456));
```

Output:

```
POINT (1.2345 2.3456)
```

Example:

```sql
SELECT ST_AsText(ST_MakePoint(1.2345, 2.3456, 3.4567));
```

Output:

```
POINT Z (1.2345 2.3456 3.4567)
```

Example:

```sql
SELECT ST_AsText(ST_MakePoint(1.2345, 2.3456, 3.4567, 4));
```

Output:

```
POINT ZM (1.2345 2.3456 3.4567 4)
```

## ST_Point

Introduction: Construct a Point from X and Y

Format: `ST_Point (X:decimal, Y:decimal)`

In `v1.4.0` an optional Z parameter was removed to be more consistent with other spatial SQL implementations.
If you are upgrading from an older version of Sedona - please use ST_PointZ or ST_PointZM to create 3D points.

SQL example:

```sql
SELECT ST_Point(double(1.2345), 2.3456)
```

Output:

```
POINT (1.2345 2.3456)
```

## ST_PointFromText

Introduction: Construct a Point from Text, delimited by Delimiter

Format: `ST_PointFromText (Text:string, Delimiter:char)`

SQL example:

```sql
SELECT ST_PointFromText('40.7128,-74.0060', ',')
```

Output:

```
POINT (40.7128 -74.006)
```

## ST_PointZ

Introduction: Construct a Point from X, Y and Z and an optional srid. If srid is not set, it defaults to 0 (unknown).
Must use ST_AsEWKT function to print the Z coordinate.

Format: `ST_PointZ (X:decimal, Y:decimal, Z:decimal)`

Format: `ST_PointZ (X:decimal, Y:decimal, Z:decimal, srid:integer)`

```sql
SELECT ST_AsEWKT(ST_PointZ(1.2345, 2.3456, 3.4567))
```

Output:

```
POINT Z(1.2345 2.3456 3.4567)
```

## ST_PointFromWKB

Introduction: Construct a Point geometry from WKB string or Binary and an optional SRID. This function also supports EWKB format.

!!!note
    Returns null if geometry is not of type Point.

Format:

`ST_PointFromWKB (Wkb: String)`

`ST_PointFromWKB (Wkb: Binary)`

`ST_PointFromWKB (Wkb: String, srid: Integer)`

`ST_PointFromWKB (Wkb: Binary, srid: Integer)`

Example:

```sql
SELECT ST_PointFromWKB([01 01 00 00 00 00 00 00 00 00 00 24 40 00 00 00 00 00 00 2e 40])
```

Output:

```
POINT (10 15)
```

## ST_PointZ

Introduction: Construct a Point from X, Y and Z and an optional srid. If srid is not set, it defaults to 0 (unknown).
Must use ST_AsEWKT function to print the Z coordinate.

Format: `ST_PointZ (X:decimal, Y:decimal, Z:decimal)`

Format: `ST_PointZ (X:decimal, Y:decimal, Z:decimal, srid:integer)`

```sql
SELECT ST_AsEWKT(ST_PointZ(1.2345, 2.3456, 3.4567))
```

Output:

```
POINT Z(1.2345 2.3456 3.4567)
```

## ST_PointFromGeoHash

Introduction: Generates a Point geometry representing the center of the GeoHash cell defined by the input string. If `precision` is not specified, the full GeoHash precision is used. Providing a `precision` value limits the GeoHash characters used to determine the Point coordinates.

Format: `ST_PointFromGeoHash(geoHash: String, precision: Integer)`

SQL Example

```sql
SELECT ST_PointFromGeoHash('s00twy01mt', 4)
```

Output:

```
POINT (0.87890625 0.966796875)
```

## ST_PolygonFromEnvelope

Introduction: Construct a Polygon from MinX, MinY, MaxX, MaxY.

Format: `ST_PolygonFromEnvelope (MinX:decimal, MinY:decimal, MaxX:decimal, MaxY:decimal)`

```sql
SELECT ST_PolygonFromEnvelope(double(1.234),double(2.234),double(3.345),double(3.345))
```

Output:

```
POLYGON ((1.234 2.234, 1.234 3.345, 3.345 3.345, 3.345 2.234, 1.234 2.234))
```

## ST_PolygonFromText

Introduction: Construct a Polygon from Text, delimited by Delimiter. Path must be closed

Format: `ST_PolygonFromText (Text:string, Delimiter:char)`

SQL example:

```sql
SELECT ST_PolygonFromText('-74.0428197,40.6867969,-74.0421975,40.6921336,-74.0508020,40.6912794,-74.0428197,40.6867969', ',')
```

Output:

```
POLYGON ((-74.0428197 40.6867969, -74.0421975 40.6921336, -74.050802 40.6912794, -74.0428197 40.6867969))
```
