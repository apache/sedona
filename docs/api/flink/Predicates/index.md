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

# Predicates

These functions test spatial relationships between geometries, returning boolean values.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_Contains](ST_Contains.md) | Return true if A fully contains B | v1.2.0 |
| [ST_CoveredBy](ST_CoveredBy.md) | Return true if A is covered by B | v1.3.0 |
| [ST_Covers](ST_Covers.md) | Return true if A covers B | v1.3.0 |
| [ST_Crosses](ST_Crosses.md) | Return true if A crosses B | v1.5.0 |
| [ST_Disjoint](ST_Disjoint.md) | Return true if A and B are disjoint | v1.2.1 |
| [ST_DWithin](ST_DWithin.md) | Returns true if 'leftGeometry' and 'rightGeometry' are within a specified 'distance'. | v1.5.1 |
| [ST_Equals](ST_Equals.md) | Return true if A equals to B | v1.5.0 |
| [ST_Intersects](ST_Intersects.md) | Return true if A intersects B | v1.2.0 |
| [ST_OrderingEquals](ST_OrderingEquals.md) | Returns true if the geometries are equal and the coordinates are in the same order | v1.2.1 |
| [ST_Overlaps](ST_Overlaps.md) | Return true if A overlaps B | v1.5.0 |
| [ST_Relate](ST_Relate.md) | The first variant of the function computes and returns the [Dimensionally Extended 9-Intersection Model (DE-9IM)](https://en.wikipedia.org/wiki/DE-9IM) matrix string representing the spatial relati... | v1.6.1 |
| [ST_RelateMatch](ST_RelateMatch.md) | This function tests the relationship between two [Dimensionally Extended 9-Intersection Model (DE-9IM)](https://en.wikipedia.org/wiki/DE-9IM) matrices representing geometry intersections. It evalua... | v1.6.1 |
| [ST_Touches](ST_Touches.md) | Return true if A touches B | v1.5.0 |
| [ST_Within](ST_Within.md) | Return true if A is within B | v1.3.0 |
