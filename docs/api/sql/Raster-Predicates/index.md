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

# Raster Predicates

These functions test spatial relationships involving raster objects.

| Function | Description | Since |
| :--- | :--- | :--- |
| [RS_Contains](RS_Contains.md) | Returns true if the geometry or raster on the left side contains the geometry or raster on the right side. The convex hull of the raster is considered in the test. | v1.5.0 |
| [RS_Intersects](RS_Intersects.md) | Returns true if raster or geometry on the left side intersects with the raster or geometry on the right side. The convex hull of the raster is considered in the test. | v1.5.0 |
| [RS_Within](RS_Within.md) | Returns true if the geometry or raster on the left side is within the geometry or raster on the right side. The convex hull of the raster is considered in the test. | v1.5.0 |
