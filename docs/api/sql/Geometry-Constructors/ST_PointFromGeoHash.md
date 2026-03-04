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

# ST_PointFromGeoHash

Introduction: Generates a Point geometry representing the center of the GeoHash cell defined by the input string. If `precision` is not specified, the full GeoHash precision is used. Providing a `precision` value limits the GeoHash characters used to determine the Point coordinates.

![ST_PointFromGeoHash](../../../image/ST_PointFromGeoHash/ST_PointFromGeoHash.svg "ST_PointFromGeoHash")

Format: `ST_PointFromGeoHash(geoHash: String, precision: Integer)`

Return type: `Geometry`

Since: `v1.6.1`

SQL Example

```sql
SELECT ST_PointFromGeoHash('s00twy01mt', 4)
```

Output:

```
POINT (0.87890625 0.966796875)
```
