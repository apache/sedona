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

# ST_MMin

Introduction: Returns M minima of the given geometry or null if there is no M coordinate.

![ST_MMin](../../../image/ST_MMin/ST_MMin.svg "ST_MMin")

Format: `ST_MMin(geom: Geometry)`

Return type: `Double`

Since: `v1.6.1`

SQL Example:

```sql
SELECT ST_MMin(
        ST_GeomFromWKT('LINESTRING ZM(1 1 1 1, 2 2 2 2, 3 3 3 3, -1 -1 -1 -1)')
)
```

Output:

```
-1.0
```
