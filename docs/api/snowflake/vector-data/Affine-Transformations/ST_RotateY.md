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

# ST_RotateY

Introduction: Performs a counter-clockwise rotation of the specified geometry around the Y-axis by the given angle measured in radians.

Format: `ST_RotateY(geometry: Geometry, angle: Double)`

SQL Example:

```sql
SELECT ST_RotateY(ST_GeomFromEWKT('SRID=4326;POLYGON ((0 0, 1 0, 1 1, 0 0))'), 10)
```

Output:

```
SRID=4326;POLYGON ((0 0, -0.8390715290764524 0, -0.8390715290764524 1, 0 0))
```
