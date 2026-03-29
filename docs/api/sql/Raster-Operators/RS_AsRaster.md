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

# RS_AsRaster

Introduction: `RS_AsRaster` converts a vector geometry into a raster dataset by assigning a specified value to all pixels covered by the geometry. Unlike `RS_Clip`, which extracts a subset of an existing raster while preserving its original values, `RS_AsRaster` generates a new raster where the geometry is rasterized onto a raster grid. The function supports all geometry types and takes the following parameters:

* `geom`: The geometry to be rasterized.
* `raster`: The reference raster to be used for overlaying the `geom` on.
* `pixelType`: Defines data type of the output raster. This can be one of the following, D (double), F (float), I (integer), S (short), US (unsigned short) or B (byte).
* `allTouched` (Since: `v1.7.1`): Decides the pixel selection criteria. If set to `true`, the function selects all pixels touched by the geometry, else, selects only pixels whose centroids intersect the geometry. Defaults to `false`.
* `value`: The value to be used for assigning pixels covered by the geometry. Defaults to using `1.0` if not provided.
* `noDataValue`: Used for assigning the no data value of the resultant raster. Defaults to `null` if not provided.
* `useGeometryExtent`: Defines the extent of the resultant raster. When set to `true`, it corresponds to the extent of `geom`, and when set to false, it corresponds to the extent of `raster`. Default value is `true` if not set.

Format:

```
RS_AsRaster(geom: Geometry, raster: Raster, pixelType: String, allTouched: Boolean, value: Double, noDataValue: Double, useGeometryExtent: Boolean)
```

```
RS_AsRaster(geom: Geometry, raster: Raster, pixelType: String, allTouched: Boolean, value: Double, noDataValue: Double)
```

```
RS_AsRaster(geom: Geometry, raster: Raster, pixelType: String, allTouched: Boolean, value: Double)
```

```
RS_AsRaster(geom: Geometry, raster: Raster, pixelType: String, allTouched: Boolean)
```

```
RS_AsRaster(geom: Geometry, raster: Raster, pixelType: String)
```

Return type: `Raster`

Since: `v1.5.0`

!!!note
    The function doesn't support rasters that have any one of the following properties:
    ```
    ScaleX < 0
    ScaleY > 0
    SkewX != 0
    SkewY != 0
    ```
    If a raster is provided with anyone of these properties then IllegalArgumentException is thrown.

For more information about ScaleX, ScaleY, SkewX, SkewY, please refer to the [Affine Transformations](../Raster-affine-transformation.md) section.

SQL Example

```sql
SELECT RS_AsRaster(
        ST_GeomFromWKT('POLYGON((15 15, 18 20, 15 24, 24 25, 15 15))'),
        RS_MakeEmptyRaster(2, 255, 255, 3, -215, 2, -2, 0, 0, 4326),
        'D', false, 255.0, 0d
    )
```

Output:

```
GridCoverage2D["g...
```

SQL Example

```sql
SELECT RS_AsRaster(
        ST_GeomFromWKT('POLYGON((15 15, 18 20, 15 24, 24 25, 15 15))'),
        RS_MakeEmptyRaster(2, 255, 255, 3, -215, 2, -2, 0, 0, 4326),
        'D'
    )
```

Output:

```
GridCoverage2D["g...
```

SQL Example

```sql
SELECT RS_AsRaster(
        ST_GeomFromWKT('POLYGON((15 15, 18 20, 15 24, 24 25, 15 15))'),
        RS_MakeEmptyRaster(2, 255, 255, 3, 215, 2, -2, 0, 0, 0),
       'D', true, 255, 0d, false
)
```

Output:

```
GridCoverage2D["g...
```
