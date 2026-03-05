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

# ST_H3KRing

Introduction: return the result of H3 function [gridDisk(cell, k)](https://h3geo.org/docs/api/traversal#griddisk).

K means `the distance of the origin index`, `gridDisk(cell, k)` return cells with distance `<=k` from the original cell.

`exactRing : Boolean`, when set to `true`, sedona will remove the result of `gridDisk(cell, k-1)` from the original results,
means only keep the cells with distance exactly `k` from the original cell

Format: `ST_H3KRing(cell: Long, k: Int, exactRing: Boolean)`

Return type: `Array<Long>`

Since: `v1.5.0`

SQL Example

```sql
SELECT ST_H3KRing(ST_H3CellIDs(ST_GeomFromWKT('POINT(1 2)'), 8, true)[0], 1, true) cells union select ST_H3KRing(ST_H3CellIDs(ST_GeomFromWKT('POINT(1 2)'), 8, true)[0], 1, false) cells
```

Output:

```
+--------------------------------------------------------------------------------------------------------------------------------------------+
|cells                                                                                                                                       |
+--------------------------------------------------------------------------------------------------------------------------------------------+
|[614552597293957119, 614552609329512447, 614552609316929535, 614552609327415295, 614552609287569407, 614552597289762815]                    |
|[614552609325318143, 614552597293957119, 614552609329512447, 614552609316929535, 614552609327415295, 614552609287569407, 614552597289762815]|
+--------------------------------------------------------------------------------------------------------------------------------------------+
```

![ST_H3KRing](../../../image/ST_H3KRing/ST_H3KRing.svg)
