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

# Measurement Functions

These functions compute measurements of distance, area, length, and angles.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_3DDistance](ST_3DDistance.md) | Return the 3-dimensional minimum cartesian distance between A and B |  |
| [ST_Angle](ST_Angle.md) | Computes and returns the angle between two vectors represented by the provided points or linestrings. |  |
| [ST_Area](ST_Area.md) | Return the area of A |  |
| [ST_AreaSpheroid](ST_AreaSpheroid.md) | Return the geodesic area of A using WGS84 spheroid. Unit is square meter. Works better for large geometries (country level) compared to `ST_Area` + `ST_Transform`. It is equivalent to PostGIS `ST_A... |  |
| [ST_Azimuth](ST_Azimuth.md) | Returns Azimuth for two given points in radians. Returns null if the two points are identical. |  |
| [ST_ClosestPoint](ST_ClosestPoint.md) | Returns the 2-dimensional point on geom1 that is closest to geom2. This is the first point of the shortest line between the geometries. If using 3D geometries, the Z coordinates will be ignored. If... |  |
| [ST_Degrees](ST_Degrees.md) | Convert an angle in radian to degrees. |  |
| [ST_Distance](ST_Distance.md) | Return the Euclidean distance between A and B |  |
| [ST_DistanceSphere](ST_DistanceSphere.md) | Return the haversine / great-circle distance of A using a given earth radius (default radius: 6371008.0). Unit is meter. Compared to `ST_Distance` + `ST_Transform`, it works better for datasets tha... |  |
| [ST_DistanceSpheroid](ST_DistanceSpheroid.md) | Return the geodesic distance of A using WGS84 spheroid. Unit is meter. Compared to `ST_Distance` + `ST_Transform`, it works better for datasets that cover large regions such as continents or the en... |  |
| [ST_FrechetDistance](ST_FrechetDistance.md) | Computes and returns discrete [Frechet Distance](https://en.wikipedia.org/wiki/Fr%C3%A9chet_distance) between the given two geometries, based on [Computing Discrete Frechet Distance](http://www.kr.... |  |
| [ST_HausdorffDistance](ST_HausdorffDistance.md) | Returns a discretized (and hence approximate) [Hausdorff distance](https://en.wikipedia.org/wiki/Hausdorff_distance) between the given 2 geometries. Optionally, a densityFraction parameter can be s... |  |
| [ST_Length](ST_Length.md) | Returns the perimeter of A. |  |
| [ST_Length2D](ST_Length2D.md) | Returns the perimeter of A. This function is an alias of [ST_Length](ST_Length.md). |  |
| [ST_LengthSpheroid](ST_LengthSpheroid.md) | Return the geodesic perimeter of A using WGS84 spheroid. Unit is meter. Works better for large geometries (country level) compared to `ST_Length` + `ST_Transform`. It is equivalent to PostGIS `ST_L... |  |
| [ST_LongestLine](ST_LongestLine.md) | Returns the LineString geometry representing the maximum distance between any two points from the input geometries. |  |
| [ST_MaxDistance](ST_MaxDistance.md) | Calculates and returns the length value representing the maximum distance between any two points across the input geometries. This function is an alias for `ST_LongestDistance`. |  |
| [ST_MinimumClearance](ST_MinimumClearance.md) | The minimum clearance is a metric that quantifies a geometry's tolerance to changes in coordinate precision or vertex positions. It represents the maximum distance by which vertices can be adjusted... |  |
| [ST_MinimumClearanceLine](ST_MinimumClearanceLine.md) | This function returns a two-point LineString geometry representing the minimum clearance distance of the input geometry. If the input geometry does not have a defined minimum clearance, such as for... |  |
| [ST_Perimeter](ST_Perimeter.md) | This function calculates the 2D perimeter of a given geometry. It supports Polygon, MultiPolygon, and GeometryCollection geometries (as long as the GeometryCollection contains polygonal geometries)... |  |
| [ST_Perimeter2D](ST_Perimeter2D.md) | This function calculates the 2D perimeter of a given geometry. It supports Polygon, MultiPolygon, and GeometryCollection geometries (as long as the GeometryCollection contains polygonal geometries)... |  |
