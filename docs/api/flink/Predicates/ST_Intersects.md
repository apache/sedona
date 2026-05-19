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

# ST_Intersects

Introduction: Return true if A intersects B

![ST_Intersects returning true](../../../image/ST_Intersects/ST_Intersects_true.svg "ST_Intersects returning true")
![ST_Intersects returning false](../../../image/ST_Intersects/ST_Intersects_false.svg "ST_Intersects returning false")

Format: `ST_Intersects (A: Geometry, B: Geometry)`

Return type: `Boolean`

Since: `v1.2.0`

Example:

```sql
SELECT ST_Intersects(ST_GeomFromWKT('LINESTRING(-43.23456 72.4567,-43.23456 72.4568)'), ST_GeomFromWKT('POINT(-43.23456 72.4567772)'))
```

Output:

```
true
```
