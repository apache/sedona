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

# ST_IsLineStringCCW

Introduction: Returns true if the input LineString's coordinate sequence has counter-clockwise ring orientation.

![ST_IsLineStringCCW](../../../image/ST_IsLineStringCCW/ST_IsLineStringCCW.svg "ST_IsLineStringCCW")

The input LineString does not need to be closed. Its existing coordinate sequence is evaluated without adding a closing coordinate. The function returns false for non-LineString geometries and LineStrings with fewer than four points.

Format: `ST_IsLineStringCCW(geom: Geometry)`

Return type: `Boolean`

Since: `v1.9.1`

SQL Example:

```sql
SELECT ST_IsLineStringCCW(ST_GeomFromWKT('LINESTRING (0 0, 1 0, 1 1, 0 1, 0 0)'))
```

Output:

```
true
```
