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

# ST_Expand

Introduction: Returns a geometry expanded from the bounding box of the input. The expansion can be specified in two ways:

1. By individual axis using `deltaX`, `deltaY`, or `deltaZ` parameters.
2. Uniformly across all axes using the `uniformDelta` parameter.

!!!Note
    Things to consider when using this function:

    1. The `uniformDelta` parameter expands Z dimensions for XYZ geometries; otherwise, it only affects XY dimensions.
    2. For XYZ geometries, specifying only `deltaX` and `deltaY` will preserve the original Z dimension.
    3. If the input geometry has an M dimension then using this function will drop the said M dimension.

Format:

`ST_Expand(geometry: Geometry, uniformDelta: Double)`

`ST_Expand(geometry: Geometry, deltaX: Double, deltaY: Double)`

`ST_Expand(geometry: Geometry, deltaX: Double, deltaY: Double, deltaZ: Double)`

Return type: `Geometry`

Since: `v1.6.1`

SQL Example:

```sql
SELECT ST_Expand(
        ST_GeomFromWKT('POLYGON Z((50 50 1, 50 80 2, 80 80 3, 80 50 2, 50 50 1))'),
        10
   )
```

Output:

```
POLYGON Z((40 40 -9, 40 90 -9, 90 90 13, 90 40 13, 40 40 -9))
```
