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
    For the geometry variant, a raster without a CRS accepts only an SRID-0 point and uses its coordinates directly. An EPSG-addressable raster requires a point SRID and transforms it when needed. A raster with a non-EPSG CRS treats an SRID-0 point as native raster coordinates, or transforms an EPSG-tagged point to the raster CRS.

Format:

`RS_Value (raster: Raster, point: Geometry)`

`RS_Value (raster: Raster, point: Geometry, band: Integer)`

`RS_Value (raster: Raster, colX: Integer, colY: Integer, band: Integer)`

Return type: `Double`

Since: `v1.4.0`

Spark SQL Examples:

- For Point Geometry:

```sql
SELECT RS_Value(raster, ST_SetSRID(ST_Point(-13077301.685, 4002565.802), RS_SRID(raster)))
FROM raster_table
```

- For Grid Coordinates:

```sql
SELECT RS_Value(raster, 3, 4, 1) FROM raster_table
```

Output:

```
5.0
```
