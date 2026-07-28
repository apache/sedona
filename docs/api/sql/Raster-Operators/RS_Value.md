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

# RS_Value

Introduction: Returns the value at the given point in the raster. If no band number is specified it defaults to 1.

!!!Note
    For the geometry variant, if neither `raster` nor `point` has a defined CRS, their coordinates are used directly. If exactly one input has a defined CRS, the function throws an error. If both inputs have the same CRS, their coordinates are used directly. Otherwise, `point` is transformed to the CRS of `raster`.

Format:

`RS_Value (raster: Raster, point: Geometry)`

`RS_Value (raster: Raster, point: Geometry, band: Integer)`

`RS_Value (raster: Raster, colX: Integer, colY: Integer, band: Integer)`

Return type: `Double`

Since: `v1.4.0`

Spark SQL Examples:

- For Point Geometry:

```sql
SELECT RS_Value(raster, ST_Point(-13077301.685, 4002565.802)) FROM raster_table
```

- For Grid Coordinates:

```sql
SELECT RS_Value(raster, 3, 4, 1) FROM raster_table
```

Output:

```
5.0
```
