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

# Geography Functions

The `Geography` type in Sedona represents spatial objects on a spherical (geodesic) model of the Earth. Unlike the planar [Geometry](../Geometry-Functions.md) type, distance, area, and other measurements performed on `Geography` objects account for the curvature of the Earth and return results in real-world units (e.g., meters).

## Geography Constructors

These functions create geography objects from various formats.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_GeogFromEWKB](Geography-Constructors/ST_GeogFromEWKB.md) | Construct a Geography from EWKB Binary. This function is an alias of [ST_GeogFromWKB](Geography-Constructors/ST_GeogFromWKB.md). | v1.8.0 |
| [ST_GeogFromEWKT](Geography-Constructors/ST_GeogFromEWKT.md) | Construct a Geography from OGC Extended WKT. | v1.8.0 |
| [ST_GeogFromGeoHash](Geography-Constructors/ST_GeogFromGeoHash.md) | Create Geography from geohash string and optional precision | v1.8.0 |
| [ST_GeogFromWKB](Geography-Constructors/ST_GeogFromWKB.md) | Construct a Geography from WKB Binary. | v1.8.0 |
| [ST_GeogFromWKT](Geography-Constructors/ST_GeogFromWKT.md) | Construct a Geography from WKT. If SRID is not set, it defaults to 0 (unknown). | v1.8.0 |
| [ST_GeogToGeometry](Geography-Constructors/ST_GeogToGeometry.md) | This function constructs a planar Geometry object from a Geography. While Sedona makes every effort to preserve the original spatial object, the conversion is not always exact because Geography and... | v1.8.0 |
| [ST_GeomToGeography](Geography-Constructors/ST_GeomToGeography.md) | This function constructs a Geography object from a planar Geometry. This function is intended for geometries defined in a Geographic Coordinate Reference System (CRS), most commonly WGS84 (EPSG:432... | v1.8.0 |

## Geography Functions

These functions operate on geography type objects.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_AsEWKT](Geography-Functions/ST_AsEWKT.md) | Return the Extended Well-Known Text representation of a geography. EWKT is an extended version of WKT which includes the SRID of the geography. The format originated in PostGIS but is supported by ... | v1.8.0 |
| [ST_Envelope](Geography-Functions/ST_Envelope.md) | This function returns the bounding box (envelope) of A. It's important to note that the bounding box is calculated using a cylindrical topology, not a spherical one. If the envelope crosses the ant... | v1.8.0 |
