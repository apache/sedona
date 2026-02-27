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

# ST_Length2D

Introduction: Returns the perimeter of A. This function is an alias of [ST_Length](ST_Length.md).

!!!Warning
    Since `v1.7.0`, this function only supports LineString, MultiLineString, and GeometryCollections containing linear geometries. Use [ST_Perimeter](ST_Perimeter.md) for polygons.

Format: ST_Length2D (A:geometry)

Since: `v1.6.1`

SQL Example:

```sql
SELECT ST_Length2D(ST_GeomFromWKT('LINESTRING(38 16,38 50,65 50,66 16,38 16)'))
```

Output:

```
123.0147027033899
```
