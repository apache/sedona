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

# ST_3DDistance

Introduction: Return the 3-dimensional minimum cartesian distance between A and B

Format: `ST_3DDistance (A: Geometry, B: Geometry)`

Since: `v1.2.0`

SQL Example

```sql
SELECT ST_3DDistance(ST_GeomFromText("POINT Z (0 0 -5)"),
                     ST_GeomFromText("POINT Z(1  1 -6"))
```

Output:

```
1.7320508075688772
```
