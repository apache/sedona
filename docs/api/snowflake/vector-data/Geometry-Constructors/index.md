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
| [ST_GeomCollFromText](ST_GeomCollFromText.md) | Constructs a GeometryCollection from the WKT with the given SRID. If SRID is not provided then it defaults to 0. It returns `null` if the WKT is not a `GEOMETRYCOLLECTION`. |  |
| [ST_GeometryFromText](ST_GeometryFromText.md) | Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown). Alias of [ST_GeomFromWKT](ST_GeomFromWKT.md) |  |
| [ST_GeomFromEWKB](ST_GeomFromEWKB.md) | Construct a Geometry from EWKB string or Binary. This function is an alias of [ST_GeomFromWKB](ST_GeomFromWKB.md). |  |
| [ST_GeomFromEWKT](ST_GeomFromEWKT.md) | Construct a Geometry from OGC Extended WKT |  |
| [ST_GeomFromGeoHash](ST_GeomFromGeoHash.md) | Create Geometry from geohash string and optional precision |  |
| [ST_GeomFromGeoJSON](ST_GeomFromGeoJSON.md) | Construct a Geometry from GeoJson |  |
| [ST_GeomFromGML](ST_GeomFromGML.md) | Construct a Geometry from GML. |  |
| [ST_GeomFromKML](ST_GeomFromKML.md) | Construct a Geometry from KML. |  |
| [ST_GeomFromText](ST_GeomFromText.md) | Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown). Alias of [ST_GeomFromWKT](ST_GeomFromWKT.md) |  |
| [ST_GeomFromWKB](ST_GeomFromWKB.md) | Construct a Geometry from WKB string or Binary. This function also supports EWKB format. |  |
| [ST_GeomFromWKT](ST_GeomFromWKT.md) | Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown). |  |
| [ST_LineFromText](ST_LineFromText.md) | Construct a Line from Wkt text |  |
| [ST_LineFromWKB](ST_LineFromWKB.md) | Construct a LineString geometry from WKB string or Binary and an optional SRID. This function also supports EWKB format. |  |
| [ST_LineStringFromText](ST_LineStringFromText.md) | Construct a LineString from Text, delimited by Delimiter |  |
| [ST_LinestringFromWKB](ST_LinestringFromWKB.md) | Construct a LineString geometry from WKB string or Binary and an optional SRID. This function also supports EWKB format and it is an alias of [ST_LineFromWKB](ST_LineFromWKB.md). |  |
| [ST_MakeEnvelope](ST_MakeEnvelope.md) | Construct a Polygon from MinX, MinY, MaxX, MaxY, and an optional SRID. |  |
| [ST_MakePoint](ST_MakePoint.md) | Creates a 2D, 3D Z or 4D ZM Point geometry. Use ST_MakePointM to make points with XYM coordinates. Z and M values are optional. |  |
| [ST_MLineFromText](ST_MLineFromText.md) | Construct a MultiLineString from Wkt. If srid is not set, it defaults to 0 (unknown). |  |
| [ST_MPointFromText](ST_MPointFromText.md) | Constructs a MultiPoint from the WKT with the given SRID. If SRID is not provided then it defaults to 0. It returns `null` if the WKT is not a `MULTIPOINT`. |  |
| [ST_MPolyFromText](ST_MPolyFromText.md) | Construct a MultiPolygon from Wkt. If srid is not set, it defaults to 0 (unknown). |  |
| [ST_Point](ST_Point.md) | Construct a Point from X and Y |  |
| [ST_PointFromGeoHash](ST_PointFromGeoHash.md) | Generates a Point geometry representing the center of the GeoHash cell defined by the input string. If `precision` is not specified, the full GeoHash precision is used. Providing a `precision` valu... |  |
| [ST_PointFromText](ST_PointFromText.md) | Construct a Point from Text, delimited by Delimiter |  |
| [ST_PointFromWKB](ST_PointFromWKB.md) | Construct a Point geometry from WKB string or Binary and an optional SRID. This function also supports EWKB format. |  |
| [ST_PointZ](ST_PointZ.md) | Construct a Point from X, Y and Z and an optional srid. If srid is not set, it defaults to 0 (unknown). Must use ST_AsEWKT function to print the Z coordinate. |  |
| [ST_PointZ](ST_PointZ.md) | Construct a Point from X, Y and Z and an optional srid. If srid is not set, it defaults to 0 (unknown). Must use ST_AsEWKT function to print the Z coordinate. |  |
| [ST_PolygonFromEnvelope](ST_PolygonFromEnvelope.md) | Construct a Polygon from MinX, MinY, MaxX, MaxY. |  |
| [ST_PolygonFromText](ST_PolygonFromText.md) | Construct a Polygon from Text, delimited by Delimiter. Path must be closed |  |
