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

# RS_PixelAsCentroids

Introduction: Returns a list of the centroid point geometry, the pixel value and its raster X and Y coordinates for each pixel in the raster at the specified band.
Each centroid represents the geometric center of the corresponding pixel's area.

Format: `RS_PixelAsCentroids(raster: Raster, band: Integer)`

Return type: `Array<Struct<geom: Geometry, val: Double, x: Integer, y: Integer>>`

Since: `v1.5.1`

SQL Example

```sql
SELECT ST_AsText(RS_PixelAsCentroids(raster, 1)) from rasters
```

Output:

```
[[POINT (-13065222 4021263.75),148.0,0,0], [POINT (-13065151 4021263.75),123.0,0,1], [POINT (-13065077 4021263.75),99.0,1,0], [POINT (-13065007 4021261.75),140.0,1,1]]
```

Spark SQL example for extracting Point, value, raster x and y coordinates:

```scala
val pointDf = sedona.read...
val rasterDf = sedona.read.format("binaryFile").load("/some/path/*.tiff")
var df = sedona.read.format("binaryFile").load("/some/path/*.tiff")
df = df.selectExpr("RS_FromGeoTiff(content) as raster")

df.selectExpr(
  "explode(RS_PixelAsCentroids(raster, 1)) as exploded"
).selectExpr(
  "exploded.geom as geom",
  "exploded.value as value",
  "exploded.x as x",
  "exploded.y as y"
).show(3)
```

Output:

```
+----------------------------------------------+-----+---+---+
|geom                                          |value|x  |y  |
+----------------------------------------------+-----+---+---+
|POINT (-13095781.835693639 4021226.5856936392)|0.0  |1  |1  |
|POINT (-13095709.507080918 4021226.5856936392)|0.0  |2  |1  |
|POINT (-13095637.178468198 4021226.5856936392)|0.0  |3  |1  |
+----------------------------------------------+-----+---+---+
```
