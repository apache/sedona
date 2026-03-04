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

# RS_PixelAsPolygon

Introduction: Returns a polygon geometry that bounds the specified pixel.
The pixel coordinates specified are 1-indexed.
If `colX` and `rowY` are out of bounds for the raster, they are interpolated assuming the same skew and translate values.

Format: `RS_PixelAsPolygon(raster: Raster, colX: Integer, rowY: Integer)`

Return type: `Geometry`

Since: `v1.5.0`

SQL Example

```sql
SELECT ST_AsText(RS_PixelAsPolygon(RS_MakeEmptyRaster(1, 5, 10, 123, -230, 8), 2, 3))
```

Output:

```
POLYGON ((131 -246, 139 -246, 139 -254, 131 -254, 131 -246))
```
