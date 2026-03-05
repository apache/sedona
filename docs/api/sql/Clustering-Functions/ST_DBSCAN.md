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

# ST_DBSCAN

Introduction: Performs a DBSCAN clustering across the entire dataframe.

Returns a struct containing the cluster ID and a boolean indicating if the record is a core point in the cluster.

- `epsilon` is the maximum distance between two points for them to be considered as part of the same cluster.
- `minPoints` is the minimum number of neighbors a single record must have to form a cluster.
- `useSpheroid` is whether to use ST_DistanceSpheroid or ST_Distance as the distance metric.

![ST_DBSCAN](../../../image/ST_DBSCAN/ST_DBSCAN.svg "ST_DBSCAN")

Format: `ST_DBSCAN(geom: Geometry, epsilon: Double, minPoints: Integer, useSpheroid: Boolean)`

Return type: `Struct<isCore: Boolean, cluster: Long>`

Since: `v1.7.1`

SQL Example

```sql
SELECT ST_DBSCAN(geom, 1.0, 2, False)
```

Output:

```
{true, 85899345920}
```
