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

# Linear Referencing

These functions work with linear referencing, measures along lines, and trajectory data.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_AddMeasure](ST_AddMeasure.md) | Computes a new geometry with measure (M) values linearly interpolated between start and end points. For geometries lacking M dimensions, M values are added. Existing M values are overwritten by the... | v1.6.1 |
| [ST_InterpolatePoint](ST_InterpolatePoint.md) | Returns the interpolated measure value of a linear measured LineString at the point closest to the specified point. | v1.7.0 |
| [ST_IsValidTrajectory](ST_IsValidTrajectory.md) | This function checks if a geometry is a valid trajectory representation. For a trajectory to be considered valid, it must be a LineString that includes measure (M) values. The key requirement is th... | v1.6.1 |
| [ST_LineInterpolatePoint](ST_LineInterpolatePoint.md) | Returns a point interpolated along a line. First argument must be a LINESTRING. Second argument is a Double between 0 and 1 representing fraction of total linestring length the point has to be loca... | v1.5.0 |
| [ST_LineLocatePoint](ST_LineLocatePoint.md) | Returns a double between 0 and 1, representing the location of the closest point on the LineString as a fraction of its total length. The first argument must be a LINESTRING, and the second argumen... | v1.5.1 |
| [ST_LineSubstring](ST_LineSubstring.md) | Return a linestring being a substring of the input one starting and ending at the given fractions of total 2d length. Second and third arguments are Double values between 0 and 1. This only works w... | v1.5.0 |
| [ST_LocateAlong](ST_LocateAlong.md) | This function computes Point or MultiPoint geometries representing locations along a measured input geometry (LineString or MultiLineString) corresponding to the provided measure value(s). Polygona... | v1.6.1 |
