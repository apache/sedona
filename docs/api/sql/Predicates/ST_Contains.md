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

# ST_Contains

Introduction: Return true if A fully contains B

![ST_Contains returning true](../../../image/ST_Contains/ST_Contains_true.svg "ST_Contains returning true")
![ST_Contains returning false](../../../image/ST_Contains/ST_Contains_false.svg "ST_Contains returning false")

Format: `ST_Contains (A: Geometry, B: Geometry)`

Return type: `Boolean`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_Contains(ST_GeomFromWKT('POLYGON((175 150,20 40,50 60,125 100,175 150))'), ST_GeomFromWKT('POINT(174 149)'))
```

Output:

```
false
```
