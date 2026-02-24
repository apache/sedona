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

# Overlay Functions

These functions compute results arising from the overlay of two geometries. These are also known as point-set theoretic boolean operations.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_Difference](ST_Difference.md) | Return the difference between geometry A and B (return part of geometry A that does not intersect geometry B) |  |
| [ST_Intersection](ST_Intersection.md) | Return the intersection geometry of A and B |  |
| [ST_Split](ST_Split.md) | Split an input geometry by another geometry (called the blade). Linear (LineString or MultiLineString) geometry can be split by a Point, MultiPoint, LineString, MultiLineString, Polygon, or MultiPo... |  |
| [ST_SubDivide](ST_SubDivide.md) | Returns a multi-geometry divided based of given maximum number of vertices. |  |
| [ST_SubDivideExplode](ST_SubDivideExplode.md) | It works the same as ST_SubDivide but returns new rows with geometries instead of a multi-geometry. |  |
| [ST_SymDifference](ST_SymDifference.md) | Return the symmetrical difference between geometry A and B (return parts of geometries which are in either of the sets, but not in their intersection) |  |
| [ST_UnaryUnion](ST_UnaryUnion.md) | This variant of [ST_Union](ST_Union.md) operates on a single geometry input. The input geometry can be a simple Geometry type, a MultiGeometry, or a GeometryCollection. The function calculates the ge... |  |
| [ST_Union](ST_Union.md) | Return the union of geometry A and B |  |
