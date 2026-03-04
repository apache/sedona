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

# RS_Values

Introduction: Returns the values at the given points or grid coordinates in the raster. If no band number is specified it defaults to 1.

RS_Values is similar to RS_Value but operates on an array of points or grid coordinates.
RS_Values can be significantly faster since a raster only has to be loaded once for several points.

!!!Note
    Since `v1.5.1`, if the coordinate reference system (CRS) of the input `points` geometries differs from that of the `raster`, then `points` will be transformed to match the CRS of the `raster`. If the `raster` or `points` doesn't have a CRS then it will default to `4326/WGS84`.

Format:

`RS_Values (raster: Raster, points: ARRAY[Geometry])`

`RS_Values (raster: Raster, points: ARRAY[Geometry], band: Integer)`

```
RS_Values (raster: Raster, xCoordinates: ARRAY[Integer], yCoordinates: ARRAY[Integer], band: Integer)
```

Return type: `Array<Double>`

Since: `v1.4.0`

SQL Example

- For Array of Point geometries:

```sql
SELECT RS_Values(raster, Array(ST_Point(-1307.5, 400.8), ST_Point(-1403.3, 399.1)))
FROM raster_table
```

- For Arrays of grid coordinates:

```sql
SELECT RS_Values(raster, Array(4, 5), Array(3, 2), 1) FROM raster_table
```

Output:

```
Array(5.0, 3.0)
```

Spark SQL example for joining a point dataset with a raster dataset:

```scala
val pointDf = sedona.read...
val rasterDf = sedona.read.format("binaryFile").load("/some/path/*.tiff")
  .withColumn("raster", expr("RS_FromGeoTiff(content)"))
  .withColumn("envelope", expr("RS_Envelope(raster)"))

// Join the points with the raster extent and aggregate points to arrays.
// We only use the path and envelope of the raster to keep the shuffle as small as possible.
val df = pointDf.join(rasterDf.select("path", "envelope"), expr("ST_Within(point_geom, envelope)"))
  .groupBy("path")
  .agg(collect_list("point_geom").alias("point"), collect_list("point_id").alias("id"))

df.join(rasterDf, "path")
  .selectExpr("explode(arrays_zip(id, point, RS_Values(raster, point))) as result")
  .selectExpr("result.*")
  .show()
```

Output:

```
+----+------------+-------+
| id | point      | value |
+----+------------+-------+
|  4 | POINT(1 1) |   3.0 |
|  5 | POINT(2 2) |   7.0 |
+----+------------+-------+
```
