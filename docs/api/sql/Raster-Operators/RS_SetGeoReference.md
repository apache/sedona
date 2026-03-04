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

# RS_SetGeoReference

Introduction: Sets the Georeference information of an object in a single call. Accepts inputs in `GDAL` and `ESRI` format.
Default format is `GDAL`. If all 6 parameters are not provided then will return null.

For more information about ScaleX, ScaleY, SkewX, SkewY, please refer to the [Affine Transformations](../Raster-affine-transformation.md) section.

Format:

```
RS_SetGeoReference(raster: Raster, geoRefCoord: String, format: String = "GDAL")
```

```
RS_SetGeoReference(raster: Raster, upperLeftX: Double, upperLeftY: Double, scaleX: Double, scaleY: Double, skewX: Double, skewY: Double)
```

Return type: `Raster`

Since: `v1.5.0`

Difference between format representation is as follows:

`GDAL`

```
ScaleX SkewY SkewX ScaleY UpperLeftX UpperLeftY
```

`ESRI`

```
ScaleX SkewY SkewX ScaleY (UpperLeftX + ScaleX * 0.5) (UpperLeftY + ScaleY * 0.5)
```

SQL Example

```sql
SELECT RS_GeoReference(
        RS_SetGeoReference(
            RS_MakeEmptyRaster(1, 20, 20, 2, 22, 2, 3, 1, 1, 0),
            '3 1.5 1.5 2 22 3'
        )
    )
```

Output:

```
3.000000
1.500000
1.500000
2.000000
22.000000
3.000000
```

SQL Example

```sql
SELECT RS_GeoReference(
        RS_SetGeoReference(
            RS_MakeEmptyRaster(1, 20, 20, 2, 22, 2, 3, 1, 1, 0),
            '3 1.5 1.5 2 22 3', 'ESRI'
        )
    )
```

Output:

```
3.000000
1.500000
1.500000
2.000000
20.500000
2.000000
```

SQL Example

```sql
SELECT RS_GeoReference(
        RS_SetGeoReference(
            RS_MakeEmptyRaster(2, 5, 5, 0, 0, 1, -1, 0, 0, 0),
            8, -3, 4, 5, 0.2, 0.2
        )
    )
```

Output:

```
4.000000
0.200000
0.200000
5.000000
8.000000
-3.000000
```
