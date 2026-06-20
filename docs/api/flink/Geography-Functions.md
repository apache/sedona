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

The `Geography` type in Sedona represents spatial objects on a spherical (geodesic) model of the Earth. Unlike the planar [Geometry](Geometry-Functions.md) type, distance, area, and other measurements performed on `Geography` objects account for the curvature of the Earth and return results in real-world units (e.g., meters).

SedonaFlink exposes geography constructors, measurement and output functions, and spatial predicates. Constructors build `Geography` values and convert between `Geometry` and `Geography`; the functions and predicates operate on `Geography` columns directly. These functions share the same names as their `Geometry` counterparts — SedonaFlink resolves the geography variant from the column type.

## Geography Constructors

These functions create geography objects from various formats, or convert between geometry and geography.

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [ST_GeogCollFromText](Geography-Constructors/ST_GeogCollFromText.md) | Geography | Constructs a GeometryCollection geography from the WKT with the given SRID. If SRID is not provided then it defaults to 0. It returns `null` if the WKT is not a `GEOMETRYCOLLECTION`. | v1.9.1 |
| [ST_GeogFromEWKB](Geography-Constructors/ST_GeogFromEWKB.md) | Geography | Construct a Geography from EWKB Binary. This function is an alias of [ST_GeogFromWKB](Geography-Constructors/ST_GeogFromWKB.md). | v1.9.1 |
| [ST_GeogFromEWKT](Geography-Constructors/ST_GeogFromEWKT.md) | Geography | Construct a Geography from OGC Extended WKT. | v1.9.1 |
| [ST_GeogFromGeoHash](Geography-Constructors/ST_GeogFromGeoHash.md) | Geography | Create Geography from geohash string and optional precision. | v1.9.1 |
| [ST_GeogFromText](Geography-Constructors/ST_GeogFromText.md) | Geography | Construct a Geography from WKT. If SRID is not set, it defaults to 0 (unknown). Alias of [ST_GeogFromWKT](Geography-Constructors/ST_GeogFromWKT.md). | v1.9.1 |
| [ST_GeogFromWKB](Geography-Constructors/ST_GeogFromWKB.md) | Geography | Construct a Geography from WKB Binary. | v1.9.1 |
| [ST_GeogFromWKT](Geography-Constructors/ST_GeogFromWKT.md) | Geography | Construct a Geography from WKT. If SRID is not set, it defaults to 0 (unknown). | v1.9.1 |
| [ST_GeogToGeometry](Geography-Constructors/ST_GeogToGeometry.md) | Geometry | Construct a planar Geometry from a Geography. | v1.9.1 |
| [ST_GeomToGeography](Geography-Constructors/ST_GeomToGeography.md) | Geography | Construct a Geography from a planar Geometry. | v1.9.1 |

## Geography Functions

These functions measure or format geography objects. Measurements are computed on a spherical model of the Earth (radius `R = 6 371 008 m`, the authalic Earth radius), not the WGS84 ellipsoid — areas in square meters and lengths/distances in meters. (`ST_Buffer` is the exception: it is computed on the WGS84 spheroid.)

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [ST_Area](Geography-Functions/ST_Area.md) | Double | Return the geodesic area of a geography in square meters. | v1.9.1 |
| [ST_AsEWKT](Geography-Functions/ST_AsEWKT.md) | String | Return the EWKT representation of a geography, including its SRID. | v1.9.1 |
| [ST_AsText](Geography-Functions/ST_AsText.md) | String | Return the WKT representation of a geography. | v1.9.1 |
| [ST_Buffer](Geography-Functions/ST_Buffer.md) | Geography | Return the geodesic buffer of a geography (distance in meters). | v1.9.1 |
| [ST_Centroid](Geography-Functions/ST_Centroid.md) | Geography | Return the centroid of a geography as a Geography point. | v1.9.1 |
| [ST_Distance](Geography-Functions/ST_Distance.md) | Double | Return the minimum geodesic distance between two geographies in meters. | v1.9.1 |
| [ST_Envelope](Geography-Functions/ST_Envelope.md) | Geography | Return the bounding box of a geography. Supports antimeridian splitting. | v1.9.1 |
| [ST_GeometryType](Geography-Functions/ST_GeometryType.md) | String | Return the type of a geography as a string. | v1.9.1 |
| [ST_Length](Geography-Functions/ST_Length.md) | Double | Return the spherical length of a geography in meters. | v1.9.1 |
| [ST_NPoints](Geography-Functions/ST_NPoints.md) | Integer | Return the number of points (vertices) in a geography. | v1.9.1 |
| [ST_NumGeometries](Geography-Functions/ST_NumGeometries.md) | Integer | Return the number of sub-geometries in a geography. | v1.9.1 |

## Geography Predicates

These functions test spatial relationships between two geographies on the sphere.

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [ST_Contains](Geography-Predicates/ST_Contains.md) | Boolean | Test whether geography A fully contains geography B. | v1.9.1 |
| [ST_DWithin](Geography-Predicates/ST_DWithin.md) | Boolean | Test whether two geographies are within a given geodesic distance (in meters) of each other. | v1.9.1 |
| [ST_Equals](Geography-Predicates/ST_Equals.md) | Boolean | Test whether two geographies are spatially equal. | v1.9.1 |
| [ST_Intersects](Geography-Predicates/ST_Intersects.md) | Boolean | Test whether two geographies intersect. | v1.9.1 |
| [ST_Within](Geography-Predicates/ST_Within.md) | Boolean | Test whether geography A is fully within geography B. | v1.9.1 |
