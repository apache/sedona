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

# RS_PixelAsPolygons

Introduction: Returns a list of the polygon geometry, the pixel value and its raster X and Y coordinates for each pixel in the raster at the specified band.

Format: `RS_PixelAsPolygons(raster: Raster, band: Integer)`

Return type: `Array<Struct<geom: Geometry, val: Double, x: Integer, y: Integer>>`

Since: `v1.5.1`

SQL Example

```sql
SELECT ST_AsText(RS_PixelAsPolygons(raster, 1)) from rasters
```

Output:

```
[[POLYGON ((123.19000244140625 -12, 127.19000244140625 -12, 127.19000244140625 -16, 123.19000244140625 -16, 123.19000244140625 -12)),0.0,1,1],
[POLYGON ((127.19000244140625 -12, 131.19000244140625 -12, 131.19000244140625 -16, 127.19000244140625 -16, 127.19000244140625 -12)),0.0,2,1],
[POLYGON ((131.19000244140625 -12, 135.19000244140625 -12, 135.19000244140625 -16, 131.19000244140625 -16, 131.19000244140625 -12)),0.0,3,1]]
```

Spark SQL example for extracting Point, value, raster x and y coordinates:

```scala
val pointDf = sedona.read...
val rasterDf = sedona.read.format("binaryFile").load("/some/path/*.tiff")
var df = sedona.read.format("binaryFile").load("/some/path/*.tiff")
df = df.selectExpr("RS_FromGeoTiff(content) as raster")

df.selectExpr(
  "explode(RS_PixelAsPolygons(raster, 1)) as exploded"
).selectExpr(
  "exploded.geom as geom",
  "exploded.value as value",
  "exploded.x as x",
  "exploded.y as y"
).show(3)
```

Output:

```
+--------------------+-----+---+---+
|                geom|value|  x|  y|
+--------------------+-----+---+---+
|POLYGON ((-130958...|  0.0|  1|  1|
|POLYGON ((-130957...|  0.0|  2|  1|
|POLYGON ((-130956...|  0.0|  3|  1|
+--------------------+-----+---+---+
```
