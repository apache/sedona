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

# RS_SetValues

Introduction: Returns a raster by replacing the values of pixels in a specified rectangular region. The top left
corner of the region is defined by the `colX` and `rowY` coordinates. The `width` and `height` parameters specify the dimensions
of the rectangular region. The new values to be assigned to the pixels in this region can be specified as an array passed
to this function.

!!!Note
    Since `v1.5.1`, if the coordinate reference system (CRS) of the input `geom` geometry differs from that of the `raster`, then `geom` will be transformed to match the CRS of the `raster`. If the `raster` or `geom` doesn't have a CRS then it will default to `4326/WGS84`.

Format without ROI `geom`:

```
RS_SetValues(raster: Raster, bandIndex: Integer, colX: Integer, rowY: Integer, width: Integer, height: Integer, newValues: ARRAY[Double])
```

```
RS_SetValues(raster: Raster, bandIndex: Integer, colX: Integer, rowY: Integer, width: Integer, height: Integer, newValues: ARRAY[Double], keepNoData: Boolean = false)
```

Return type: `Raster`

Since: `v1.5.0`

The `colX`, `rowY`, and `bandIndex` are 1-indexed. If `keepNoData` is `true`, the pixels with NoData value will not be
set to the corresponding value in `newValues`. The `newValues` should be provided in rows.

The geometry variant of this function accepts all types of Geometries, and it sets the `newValue` in the specified region under the `geom`.

For polygon geometries, the `allTouched` parameter (Since `v1.7.1`) determines how pixels are selected:

- When true, pixel selection also includes cells entered by the polygon's boundary, following the [RS_AsRaster](RS_AsRaster.md) rules.
- When false (default), pixels whose centroid intersects the polygon will be included.

A polygon never selects a pixel it touches only on the pixel's boundary, whichever edge that is. A polygon aligned with grid lines selects only pixels inside it.

Point geometries select the single pixel containing the point under the half-open rule, regardless of `allTouched`. Line rasterization follows the cells entered by its segments rather than requiring a pixel-centre intersection. A point or line lying on an internal grid edge selects the pixel after it in pixel order; one lying on the raster's last edge selects none. A neighboring pixel contacted only at a line endpoint is not selected unless the segment enters it.

For `LineString` geometries, `allTouched` can also change the rasterization window at grid boundaries and therefore the selected pixels.

!!!note
    If the shape of `newValues` doesn't match with provided `width` and `height`, `IllegalArgumentException` is thrown.

!!!Note
    If the mentioned `bandIndex` doesn't exist, this will throw an `IllegalArgumentException`.

Format with ROI `geom`:

```
RS_SetValues(raster: Raster, bandIndex: Integer, geom: Geometry, newValue: Double)
```

```
RS_SetValues(raster: Raster, bandIndex: Integer, geom: Geometry, newValue: Double, allTouched: Boolean = false)
```

```
RS_SetValues(raster: Raster, bandIndex: Integer, geom: Geometry, newValue: Double, allTouched: Boolean = false, keepNoData: Boolean = false)
```

SQL Example

```sql
SELECT RS_BandAsArray(
        RS_SetValues(
            RS_AddBandFromArray(
                RS_MakeEmptyRaster(1, 5, 5, 0, 0, 1, -1, 0, 0, 0),
                Array(1,1,1,0,0,0,1,2,3,3,5,6,7,0,0,3,0,0,3,0,0,0,0,0,0), 1, 0d
                ),
            1, 2, 2, 3, 3, [11,12,13,14,15,16,17,18,19]
            )
        )
```

Output:

```
Array(1.0, 1.0, 1.0, 0.0, 0.0, 0.0, 11.0, 12.0, 13.0, 3.0, 5.0, 14.0, 15.0, 16.0, 0.0, 3.0, 17.0, 18.0, 19.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
```

SQL Example

```sql
SELECT RS_BandAsArray(
        RS_SetValues(
            RS_AddBandFromArray(
                RS_MakeEmptyRaster(1, 5, 5, 1, -1, 1, -1, 0, 0, 0),
                Array(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0), 1
                ),
            1, ST_GeomFromWKT('POLYGON((1 -1, 3 -3, 6 -6, 4 -1, 1 -1))'), 255, false, false
            )
           )
```

Output:

```
Array(255.0, 255.0, 255.0, 0.0, 0.0, 0.0, 255.0, 255.0, 255.0, 0.0, 0.0, 0.0, 255.0, 255.0, 0.0, 0.0, 0.0, 0.0, 255.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
```
