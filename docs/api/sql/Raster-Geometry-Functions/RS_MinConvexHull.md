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

# RS_MinConvexHull

Introduction: Returns the min convex hull geometry of the raster **excluding** the NoDataBandValue band pixels, in the given band.
If no band is specified, all the bands are considered when creating the min convex hull of the raster.
The created geometry representing the min convex hull has world coordinates of the raster in its CRS as the corner coordinates.

!!!Note
    If the specified band does not exist in the raster, RS_MinConvexHull throws an IllegalArgumentException

Format:

`RS_MinConvexHull(raster: Raster)`

`RS_MinConvexHull(raster: Raster, band: Integer)`

Since: `v1.5.0`

SQL Example

```scala
val inputDf = Seq((Seq(0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0),
        Seq(0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0))).toDF("values2", "values1")
inputDf.selectExpr("ST_AsText(RS_MinConvexHull(RS_AddBandFromArray(" +
        "RS_AddBandFromArray(RS_MakeEmptyRaster(2, 5, 5, 0, 0, 1, -1, 0, 0, 0), values1, 1, 0), values2, 2, 0))) as minConvexHullAll").show()
```

Output:

```sql
+----------------------------------------+
|minConvexHullAll                        |
+----------------------------------------+
|POLYGON ((0 -1, 4 -1, 4 -5, 0 -5, 0 -1))|
+----------------------------------------+
```

SQL Example

```scala
val inputDf = Seq((Seq(0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0),
        Seq(0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0))).toDF("values2", "values1")
inputDf.selectExpr("ST_AsText(RS_MinConvexHull(RS_AddBandFromArray(" +
  "RS_AddBandFromArray(RS_MakeEmptyRaster(2, 5, 5, 0, 0, 1, -1, 0, 0, 0), values1, 1, 0), values2, 2, 0), 1)) as minConvexHull1").show()
```

Output:

```sql
+----------------------------------------+
|minConvexHull1                          |
+----------------------------------------+
|POLYGON ((1 -1, 4 -1, 4 -5, 1 -5, 1 -1))|
+----------------------------------------+
```

SQL Example

```sql
SELECT RS_MinConvexHull(raster, 3) from rasters;
```

Output:

```sql
Provided band index 3 does not lie in the raster
```
