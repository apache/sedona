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

# Bounding Box Functions

These functions produce or operate on bounding boxes and compute extent values.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_BoundingDiagonal](ST_BoundingDiagonal.md) | Returns a linestring spanning minimum and maximum values of each dimension of the given geometry's coordinates as its start and end point respectively. If an empty geometry is provided, the returne... | v1.5.0 |
| [ST_Envelope](ST_Envelope.md) | Return the envelope boundary of A | v1.0.0 |
| [ST_Expand](ST_Expand.md) | Returns a geometry expanded from the bounding box of the input. The expansion can be specified in two ways: | v1.6.1 |
| [ST_MMax](ST_MMax.md) | Returns M maxima of the given geometry or null if there is no M coordinate. | v1.6.1 |
| [ST_MMin](ST_MMin.md) | Returns M minima of the given geometry or null if there is no M coordinate. | v1.6.1 |
| [ST_XMax](ST_XMax.md) | Returns the maximum X coordinate of a geometry | v1.2.1 |
| [ST_XMin](ST_XMin.md) | Returns the minimum X coordinate of a geometry | v1.2.1 |
| [ST_YMax](ST_YMax.md) | Return the minimum Y coordinate of A | v1.2.1 |
| [ST_YMin](ST_YMin.md) | Return the minimum Y coordinate of A | v1.2.1 |
| [ST_ZMax](ST_ZMax.md) | Returns Z maxima of the given geometry or null if there is no Z coordinate. | v1.3.1 |
| [ST_ZMin](ST_ZMin.md) | Returns Z minima of the given geometry or null if there is no Z coordinate. | v1.3.1 |
