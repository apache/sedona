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
| [ST_LineInterpolatePoint](ST_LineInterpolatePoint.md) | Returns a point interpolated along a line. First argument must be a LINESTRING. Second argument is a Double between 0 and 1 representing fraction of total linestring length the point has to be loca... |  |
| [ST_LineLocatePoint](ST_LineLocatePoint.md) | Returns a double between 0 and 1, representing the location of the closest point on the LineString as a fraction of its total length. The first argument must be a LINESTRING, and the second argumen... |  |
| [ST_LineSubstring](ST_LineSubstring.md) | Return a linestring being a substring of the input one starting and ending at the given fractions of total 2d length. Second and third arguments are Double values between 0 and 1. This only works w... |  |
