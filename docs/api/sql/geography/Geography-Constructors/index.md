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

# Geography Constructors

These functions create geography objects from various formats.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_GeogFromEWKB](ST_GeogFromEWKB.md) | Construct a Geography from EWKB Binary. This function is an alias of [ST_GeogFromWKB](ST_GeogFromWKB.md). | v1.8.0 |
| [ST_GeogFromEWKT](ST_GeogFromEWKT.md) | Construct a Geography from OGC Extended WKT. | v1.8.0 |
| [ST_GeogFromGeoHash](ST_GeogFromGeoHash.md) | Create Geography from geohash string and optional precision | v1.8.0 |
| [ST_GeogFromWKB](ST_GeogFromWKB.md) | Construct a Geography from WKB Binary. | v1.8.0 |
| [ST_GeogFromWKT](ST_GeogFromWKT.md) | Construct a Geography from WKT. If SRID is not set, it defaults to 0 (unknown). | v1.8.0 |
| [ST_GeogToGeometry](ST_GeogToGeometry.md) | This function constructs a planar Geometry object from a Geography. While Sedona makes every effort to preserve the original spatial object, the conversion is not always exact because Geography and... | v1.8.0 |
| [ST_GeomToGeography](ST_GeomToGeography.md) | This function constructs a Geography object from a planar Geometry. This function is intended for geometries defined in a Geographic Coordinate Reference System (CRS), most commonly WGS84 (EPSG:432... | v1.8.0 |
