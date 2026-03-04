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

# ST_GeometryType

Introduction: Returns the type of the geometry as a string. EG: 'ST_Linestring', 'ST_Polygon' etc.

![ST_GeometryType](../../../image/ST_GeometryType/ST_GeometryType.svg "ST_GeometryType")

Format: `ST_GeometryType (A: Geometry)`

Return type: `String`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_GeometryType(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'))
```

Output:

```
ST_LineString
```
