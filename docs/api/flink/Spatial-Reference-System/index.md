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

# Spatial Reference System

These functions work with the Spatial Reference System of geometries.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_BestSRID](ST_BestSRID.md) | Returns the estimated most appropriate Spatial Reference Identifier (SRID) for a given geometry, based on its spatial extent and location. It evaluates the geometry's bounding envelope and selects ... | v1.6.0 |
| [ST_SetSRID](ST_SetSRID.md) | Sets the spatial reference system identifier (SRID) of the geometry. | v1.3.0 |
| [ST_SRID](ST_SRID.md) | Return the spatial reference system identifier (SRID) of the geometry. | v1.3.0 |
| [ST_Transform](ST_Transform.md) | Transform the Spatial Reference System / Coordinate Reference System of A, from SourceCRS to TargetCRS. | v1.2.0 |
