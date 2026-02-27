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

# ST_WeightedDistanceBandColumn

Introduction: Introduction: Returns a `weights` column containing every record in a dataframe within a specified `threshold` distance.

The `weights` column is an array of structs containing the `attributes` from each neighbor and that neighbor's weight. Since this is a distance weighted distance band, weights will be distance^alpha.

Format: `ST_WeightedDistanceBandColumn(geometry:Geometry, threshold: Double, alpha: Double, includeZeroDistanceNeighbors: boolean, includeSelf: boolean, selfWeight: Double, useSpheroid: boolean, attributes: Struct)`

Since: `v1.7.1`

SQL Example

```sql
ST_WeightedDistanceBandColumn(geometry, 1.0, -1.0, true, true, 1.0, false, struct(id, geometry))
```

Output:

```sql
{% raw %}
[{{15, POINT (3 1.9)}, 1.0}, {{16, POINT (3 2)}, 9.999999999999991}, {{17, POINT (3 2.1)}, 4.999999999999996}, {{18, POINT (3 2.2)}, 3.3333333333333304}]
{% endraw %}
```
