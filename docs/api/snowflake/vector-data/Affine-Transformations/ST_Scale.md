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

# ST_Scale

Introduction: This function scales the geometry to a new size by multiplying the ordinates with the corresponding scaling factors provided as parameters `scaleX` and `scaleY`.

!!!Note
    This function is designed for scaling 2D geometries. While it currently doesn't support scaling the Z and M coordinates, it preserves these values during the scaling operation.

![ST_Scale](../../../../image/ST_Scale/ST_Scale.svg "ST_Scale")

Format: `ST_Scale(geometry: Geometry, scaleX: Double, scaleY: Double)`

Return type: `Geometry`

SQL Example:

```sql
SELECT ST_Scale(
        ST_GeomFromWKT('POLYGON ((0 0, 0 1.5, 1.5 1.5, 1.5 0, 0 0))'),
       3, 2
)
```

Output:

```
POLYGON ((0 0, 0 3, 4.5 3, 4.5 0, 0 0))
```
