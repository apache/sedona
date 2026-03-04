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

# ST_LocalOutlierFactor

Introduction: Computes the Local Outlier Factor (LOF) for each point in the input dataset.

Local Outlier Factor is an algorithm for determining the degree to which a single record is an inlier or outlier. It is
based on how close a record is to its `k` nearest neighbors vs how close those neighbors are to their `k` nearest
neighbors. Values substantially less than `1` imply that the record is an inlier, while values greater than `1` imply that
the record is an outlier.

!!!Note
    ST_LocalOutlierFactor has a useSphere parameter rather than a useSpheroid parameter. This function thus uses a spherical model of the earth rather than an ellipsoidal model when calculating distance.

Format: `ST_LocalOutlierFactor(geometry: Geometry, k: Int, useSphere: Boolean)`

Return type: `Double`

Since: `v1.7.1`

SQL Example

```sql
SELECT ST_LocalOutlierFactor(geometry, 5, true)
```

Output:

```
1.0009256283408587
```
