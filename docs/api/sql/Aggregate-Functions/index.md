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

# Aggregate Functions

These functions perform aggregate operations on groups of geometries.

| Function | Description | Since |
| :--- | :--- | :--- |
| [ST_Collect_Agg](ST_Collect_Agg.md) | Collects all geometries in a geometry column into a single multi-geometry (MultiPoint, MultiLineString, MultiPolygon, or GeometryCollection). Unlike `ST_Union_Agg`, this function does not dissolve ... | v1.8.1 |
| [ST_Envelope_Agg](ST_Envelope_Agg.md) | Return the entire envelope boundary of all geometries in A. Empty geometries and null values are skipped. If all inputs are empty or null, the result is null. This behavior is consistent with PostG... | v1.0.0 |
| [ST_Intersection_Agg](ST_Intersection_Agg.md) | Return the polygon intersection of all polygons in A | v1.0.0 |
| [ST_Union_Agg](ST_Union_Agg.md) | Return the polygon union of all polygons in A | v1.0.0 |
