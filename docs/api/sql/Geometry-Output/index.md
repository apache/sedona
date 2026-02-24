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

# Geometry Output

These functions convert geometry objects into various textual or binary formats.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_AsBinary](ST_AsBinary.md) | Return the Well-Known Binary representation of a geometry | v1.1.1 |
| [ST_AsEWKB](ST_AsEWKB.md) | Return the Extended Well-Known Binary representation of a geometry. EWKB is an extended version of WKB which includes the SRID of the geometry. The format originated in PostGIS but is supported by ... | v1.1.1 |
| [ST_AsEWKT](ST_AsEWKT.md) | Return the Extended Well-Known Text representation of a geometry. EWKT is an extended version of WKT which includes the SRID of the geometry. The format originated in PostGIS but is supported by ma... | v1.2.1 |
| [ST_AsGeoJSON](ST_AsGeoJSON.md) | Return the [GeoJSON](https://geojson.org/) string representation of a geometry | v1.6.1 |
| [ST_AsGML](ST_AsGML.md) | Return the [GML](https://www.ogc.org/standards/gml) string representation of a geometry | v1.3.0 |
| [ST_AsHEXEWKB](ST_AsHEXEWKB.md) | This function returns the input geometry encoded to a text representation in HEXEWKB format. The HEXEWKB encoding can use either little-endian (NDR) or big-endian (XDR) byte ordering. If no encodin... | v1.6.1 |
| [ST_AsKML](ST_AsKML.md) | Return the [KML](https://www.ogc.org/standards/kml) string representation of a geometry | v1.3.0 |
| [ST_AsText](ST_AsText.md) | Return the Well-Known Text string representation of a geometry. It will support M coordinate if present since v1.5.0. | v1.0.0 |
| [ST_GeoHash](ST_GeoHash.md) | Returns GeoHash of the geometry with given precision | v1.1.1 |
