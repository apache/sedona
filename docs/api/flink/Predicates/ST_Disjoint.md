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

# ST_Disjoint

Introduction: Return true if A and B are disjoint

![ST_Disjoint returning true](../../../image/ST_Disjoint/ST_Disjoint_true.svg "ST_Disjoint returning true")
![ST_Disjoint returning false](../../../image/ST_Disjoint/ST_Disjoint_false.svg "ST_Disjoint returning false")

Format: `ST_Disjoint (A: Geometry, B: Geometry)`

Return type: `Boolean`

Since: `v1.2.1`

Example:

```sql
SELECT ST_Disjoint(ST_GeomFromWKT('POLYGON((1 4, 4.5 4, 4.5 2, 1 2, 1 4))'),ST_GeomFromWKT('POLYGON((5 4, 6 4, 6 2, 5 2, 5 4))'))
```

Output:

```
true
```
