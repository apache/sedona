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

# ST_H3CellDistance

Introduction: return result of h3 function [gridDistance(cel1, cell2)](https://h3geo.org/docs/api/traversal#griddistance).
As described by H3 documentation
> Finding the distance can fail because the two indexes are not comparable (different resolutions), too far apart, or are separated by pentagonal distortion. This is the same set of limitations as the local IJ coordinate space functions.

In this case, Sedona use in-house implementation of estimation the shortest path and return the size as distance.

Format: `ST_H3CellDistance(cell1: Long, cell2: Long)`

Since: `v1.5.0`

Example:

```sql
select ST_H3CellDistance(ST_H3CellIDs(ST_GeomFromWKT('POINT(1 2)'), 8, true)[1], ST_H3CellIDs(ST_GeomFromWKT('POINT(1.23 1.59)'), 8, true)[1])
```

Output:

```
+----+----------------------+
| op |               EXPR$0 |
+----+----------------------+
| +I |                   78 |
+----+----------------------+
```
