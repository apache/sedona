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

# ST_GLocal

Introduction: Runs Getis and Ord's G Local (Gi or Gi*) statistic on the geometry given the `weights` and `level`.

Getis and Ord's Gi and Gi* statistics are used to identify data points with locally high values (hot spots) and low
values (cold spots) in a spatial dataset.

The `ST_WeightedDistanceBand` and `ST_BinaryDistanceBand` functions can be used to generate the `weights` column.

Format: `ST_GLocal(geom: Geometry, weights: Struct, level: Int)`

Since: `v1.7.1`

SQL Example

```sql
ST_GLocal(myVariable, ST_BinaryDistanceBandColumn(geometry, 1.0, true, true, false, struct(myVariable, geometry)), true)
```

Output:

```
{0.5238095238095238, 0.4444444444444444, 0.001049802637104223, 2.4494897427831814, 0.00715293921771476}
```
