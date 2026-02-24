<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Geometry Constructors

These functions create geometry objects from various textual or binary formats, or from coordinate values.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_GeomCollFromText](ST_GeomCollFromText.md) | Constructs a GeometryCollection from the WKT with the given SRID. If SRID is not provided then it defaults to 0. It returns `null` if the WKT is not a `GEOMETRYCOLLECTION`. | v1.6.1 |
| [ST_GeometryFromText](ST_GeometryFromText.md) | Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown). Alias of [ST_GeomFromWKT](ST_GeomFromWKT.md) | v1.6.1 |
| [ST_GeomFromEWKB](ST_GeomFromEWKB.md) | Construct a Geometry from EWKB string or Binary. This function is an alias of [ST_GeomFromWKB](ST_GeomFromWKB.md). | v1.6.1 |
| [ST_GeomFromEWKT](ST_GeomFromEWKT.md) | Construct a Geometry from OGC Extended WKT | v1.5.0 |
| [ST_GeomFromGeoHash](ST_GeomFromGeoHash.md) | Create Geometry from geohash string and optional precision | v1.1.1 |
| [ST_GeomFromGeoJSON](ST_GeomFromGeoJSON.md) | Construct a Geometry from GeoJson | v1.0.0 |
| [ST_GeomFromGML](ST_GeomFromGML.md) | Construct a Geometry from GML. | v1.3.0 |
| [ST_GeomFromKML](ST_GeomFromKML.md) | Construct a Geometry from KML. | v1.3.0 |
| [ST_GeomFromMySQL](ST_GeomFromMySQL.md) | Construct a Geometry from MySQL Geometry binary. | v1.8.0 |
| [ST_GeomFromText](ST_GeomFromText.md) | Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown). Alias of [ST_GeomFromWKT](ST_GeomFromWKT.md) | v1.0.0 |
| [ST_GeomFromWKB](ST_GeomFromWKB.md) | Construct a Geometry from WKB string or Binary. This function also supports EWKB format. | v1.0.0 |
| [ST_GeomFromWKT](ST_GeomFromWKT.md) | Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown). | v1.0.0 |
| [ST_LineFromText](ST_LineFromText.md) | Construct a Line from Wkt text | v1.2.1 |
| [ST_LineFromWKB](ST_LineFromWKB.md) | Construct a LineString geometry from WKB string or Binary and an optional SRID. This function also supports EWKB format. | v1.6.1 |
| [ST_LineStringFromText](ST_LineStringFromText.md) | Construct a LineString from Text, delimited by Delimiter | v1.0.0 |
| [ST_LinestringFromWKB](ST_LinestringFromWKB.md) | Construct a LineString geometry from WKB string or Binary and an optional SRID. This function also supports EWKB format and it is an alias of [ST_LineFromWKB](ST_LineFromWKB.md). | v1.6.1 |
| [ST_MakeEnvelope](ST_MakeEnvelope.md) | Construct a Polygon from MinX, MinY, MaxX, MaxY, and an optional SRID. | v1.7.0 |
| [ST_MakePoint](ST_MakePoint.md) | Creates a 2D, 3D Z or 4D ZM Point geometry. Use [ST_MakePointM](ST_MakePointM.md) to make points with XYM coordinates. Z and M values are optional. | v1.5.0 |
| [ST_MakePointM](ST_MakePointM.md) | Creates a point with X, Y, and M coordinate. Use [ST_MakePoint](ST_MakePoint.md) to make points with XY, XYZ, or XYZM coordinates. | v1.6.1 |
| [ST_MLineFromText](ST_MLineFromText.md) | Construct a MultiLineString from Wkt. If srid is not set, it defaults to 0 (unknown). | v1.3.1 |
| [ST_MPointFromText](ST_MPointFromText.md) | Constructs a MultiPoint from the WKT with the given SRID. If SRID is not provided then it defaults to 0. It returns `null` if the WKT is not a `MULTIPOINT`. | v1.6.1 |
| [ST_MPolyFromText](ST_MPolyFromText.md) | Construct a MultiPolygon from Wkt. If srid is not set, it defaults to 0 (unknown). | v1.3.1 |
| [ST_Point](ST_Point.md) | Construct a Point from X and Y | v1.0.0 |
| [ST_PointFromGeoHash](ST_PointFromGeoHash.md) | Generates a Point geometry representing the center of the GeoHash cell defined by the input string. If `precision` is not specified, the full GeoHash precision is used. Providing a `precision` valu... | v1.6.1 |
| [ST_PointFromText](ST_PointFromText.md) | Construct a Point from Text, delimited by Delimiter | v1.0.0 |
| [ST_PointFromWKB](ST_PointFromWKB.md) | Construct a Point geometry from WKB string or Binary and an optional SRID. This function also supports EWKB format. | v1.6.1 |
| [ST_PointM](ST_PointM.md) | Construct a Point from X, Y and M and an optional srid. If srid is not set, it defaults to 0 (unknown). Must use ST_AsEWKT function to print the Z and M coordinates. | v1.6.1 |
| [ST_PointZ](ST_PointZ.md) | Construct a Point from X, Y and Z and an optional srid. If srid is not set, it defaults to 0 (unknown). Must use ST_AsEWKT function to print the Z coordinate. | v1.4.0 |
| [ST_PointZM](ST_PointZM.md) | Construct a Point from X, Y, Z, M and an optional srid. If srid is not set, it defaults to 0 (unknown). Must use ST_AsEWKT function to print the Z and M coordinates. | v1.6.1 |
| [ST_PolygonFromEnvelope](ST_PolygonFromEnvelope.md) | Construct a Polygon from MinX, MinY, MaxX, MaxY. | v1.0.0 |
| [ST_PolygonFromText](ST_PolygonFromText.md) | Construct a Polygon from Text, delimited by Delimiter. Path must be closed | v1.0.0 |
