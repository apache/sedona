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
    Since `v1.5.1`, if the coordinate reference system (CRS) of the input `point` geometry differs from that of the `raster`, then `point` will be transformed to match the CRS of the `raster`. If the `raster` or `point` doesn't have a CRS then it will default to `4326/WGS84`.

Format:

`RS_Value (raster: Raster, point: Geometry)`

`RS_Value (raster: Raster, point: Geometry, band: Integer)`

`RS_Value (raster: Raster, colX: Integer, colY: Integer, band: Integer)`

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
