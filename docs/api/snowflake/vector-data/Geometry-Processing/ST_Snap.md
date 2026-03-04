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

# ST_Snap

Introduction: Snaps the vertices and segments of the `input` geometry to `reference` geometry within the specified `tolerance` distance. The `tolerance` parameter controls the maximum snap distance.

If the minimum distance between the geometries exceeds the `tolerance`, the `input` geometry is returned unmodified. Adjusting the `tolerance` value allows tuning which vertices should snap to the `reference` and which remain untouched.

Format: `ST_Snap(input: Geometry, reference: Geometry, tolerance: double)`

Return type: `Geometry`

Input geometry:

![ST_Snap Base example](../../../../image/st_snap/st-snap-base-example.png "ST_Snap Base example")

SQL Example:

```sql
SELECT
    ST_Snap(poly, line, ST_Distance(poly, line) * 1.01) AS polySnapped FROM (
        SELECT ST_GeomFromWKT('POLYGON ((236877.58 -6.61, 236878.29 -8.35, 236879.98 -8.33, 236879.72 -7.63, 236880.35 -6.62, 236877.58 -6.61), (236878.45 -7.01, 236878.43 -7.52, 236879.29 -7.50, 236878.63 -7.22, 236878.76 -6.89, 236878.45 -7.01))') as poly,
            ST_GeomFromWKT('LINESTRING (236880.53 -8.22, 236881.15 -7.68, 236880.69 -6.81)') as line
)
```

Output:

![ST_Snap applied example](../../../../image/st_snap/st-snap-applied.png "ST_Snap applied example")

```
POLYGON ((236877.58 -6.61, 236878.29 -8.35, 236879.98 -8.33, 236879.72 -7.63, 236880.69 -6.81, 236877.58 -6.61), (236878.45 -7.01, 236878.43 -7.52, 236879.29 -7.5, 236878.63 -7.22, 236878.76 -6.89, 236878.45 -7.01))
```
