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

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [ST_GeogFromEWKB](Geography-Constructors/ST_GeogFromEWKB.md) | Geography | Construct a Geography from EWKB Binary. This function is an alias of [ST_GeogFromWKB](Geography-Constructors/ST_GeogFromWKB.md). | v1.8.0 |
| [ST_GeogFromEWKT](Geography-Constructors/ST_GeogFromEWKT.md) | Geography | Construct a Geography from OGC Extended WKT. | v1.8.0 |
| [ST_GeogFromGeoHash](Geography-Constructors/ST_GeogFromGeoHash.md) | Geography | Create Geography from geohash string and optional precision | v1.8.0 |
| [ST_GeogFromWKB](Geography-Constructors/ST_GeogFromWKB.md) | Geography | Construct a Geography from WKB Binary. | v1.8.0 |
| [ST_GeogFromWKT](Geography-Constructors/ST_GeogFromWKT.md) | Geography | Construct a Geography from WKT. If SRID is not set, it defaults to 0 (unknown). | v1.8.0 |
| [ST_GeogToGeometry](Geography-Constructors/ST_GeogToGeometry.md) | Geometry | This function constructs a planar Geometry object from a Geography. While Sedona makes every effort to preserve the original spatial object, the conversion is not always exact because Geography and... | v1.8.0 |
| [ST_GeomToGeography](Geography-Constructors/ST_GeomToGeography.md) | Geography | This function constructs a Geography object from a planar Geometry. This function is intended for geometries defined in a Geographic Coordinate Reference System (CRS), most commonly WGS84 (EPSG:432... | v1.8.0 |

## Geography Functions

These functions operate on geography type objects.

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [ST_Area](Geography-Functions/ST_Area.md) | Double | Return the geodesic area of a geography in square meters (WGS84 spheroid). | v1.9.1 |
| [ST_AsEWKT](Geography-Functions/ST_AsEWKT.md) | String | Return the Extended Well-Known Text representation of a geography. | v1.8.0 |
| [ST_AsText](Geography-Functions/ST_AsText.md) | String | Return the Well-Known Text (WKT) representation of a geography. | v1.9.1 |
| [ST_Centroid](Geography-Functions/ST_Centroid.md) | Geography | Return the planar centroid of a geography as a Geography point (computed in projected lon/lat space). | v1.9.1 |
| [ST_Buffer](Geography-Functions/ST_Buffer.md) | Geography | Return the metric ε-buffer of a geography. Distance is always interpreted as meters along the spheroid. | v1.9.1 |
| [ST_Envelope](Geography-Functions/ST_Envelope.md) | Geography | Return the bounding box (envelope) of a geography. Supports anti-meridian splitting. | v1.8.0 |
| [ST_GeometryType](Geography-Functions/ST_GeometryType.md) | String | Return the type of a geography as a string (e.g., "ST_Point", "ST_Polygon"). | v1.9.1 |
| [ST_NPoints](Geography-Functions/ST_NPoints.md) | Integer | Return the number of points (vertices) in a geography. | v1.9.0 |
| [ST_NumGeometries](Geography-Functions/ST_NumGeometries.md) | Integer | Return the number of sub-geometries in a geography (1 for single geometries). | v1.9.1 |
| [ST_Distance](Geography-Functions/ST_Distance.md) | Double | Return the minimum geodesic distance between two geographies in meters. | v1.9.0 |
| [ST_Length](Geography-Functions/ST_Length.md) | Double | Return the spherical length of a geography in meters, summed along great-circle edges. | v1.9.1 |
| [ST_Contains](Geography-Functions/ST_Contains.md) | Boolean | Test whether geography A fully contains geography B. | v1.9.0 |
| [ST_DWithin](Geography-Functions/ST_DWithin.md) | Boolean | Test whether two geographies are within a given geodesic distance (in meters) of each other. | v1.9.1 |
| [ST_Within](Geography-Functions/ST_Within.md) | Boolean | Test whether geography A is fully within geography B. | v1.9.1 |
| [ST_Equals](Geography-Functions/ST_Equals.md) | Boolean | Test whether two geographies are spatially equal. | v1.9.1 |
