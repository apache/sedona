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
* `allTouched` (Since: `v1.7.1`): For polygons, `false` selects pixels whose centroids intersect the polygon; `true` also includes cells entered by the polygon's boundary. For lines, `true` also includes pixels that a segment meets at a single point. Defaults to `false`. Point and line selection follows the rules below.
* `value`: The value to be used for assigning pixels covered by the geometry. Defaults to using `1.0` if not provided.
* `noDataValue`: The no data value of the resultant raster. Every pixel the geometry does not cover (including interior holes) is filled with the resolved no data value, which is also recorded as the output band's no data metadata. The value must be representable in `pixelType`: an out-of-range or fractional value for an integer `pixelType`, or a value that is not exactly representable in a floating-point `pixelType` (for example `0.1` in a 32-bit float band), is rejected with an error rather than silently coerced. `NaN` is supported as a no data value for `F` and `D` pixel types; infinite no data values are not supported. When `noDataValue` is not provided, it is inherited from the reference `raster`'s first band, and `RS_AsRaster` errors if that band has no no data value. The programmatic API additionally accepts an explicit `null` noDataValue, which produces a raster with no no data value and a `0` background.
* `useGeometryExtent`: Defines the extent of the resultant raster. When set to `true`, it corresponds to the extent of `geom`, and when set to false, it corresponds to the extent of `raster`. Default value is `true` if not set. When set to `true` and `geom` neither burns nor overlaps any pixel of `raster`, for example a polygon that only touches its boundary or a line along its right or bottom edge, there is nothing to crop to and `RS_AsRaster` returns `null`. A polygon that overlaps a pixel without covering its centre still returns its cropped, unburned raster.

Pixel selection depends on the geometry type:

* A polygon never selects a pixel it touches only on the pixel's boundary, whichever edge that is. A polygon whose edges lie on grid lines selects only pixels inside it.
* A point selects its containing half-open pixel, regardless of `allTouched`. A pixel contains its first edges in pixel order (the left and top edges on a north-up raster) but not its last. A point on an internal edge or corner selects the pixel after it in pixel order; a point on the raster's last edge selects none.
* A line selects every pixel its segments pass through; a segment does not need to reach the pixel's centre. A segment lying on a grid line selects the pixels after it in pixel order; one lying on the raster's last edge selects none. With `allTouched=false`, a pixel that a segment meets only at a single point, at one of its ends or at a pixel corner it crosses, is not selected.
* With `allTouched=true`, a line selects the pixels that GDAL selects with its `ALL_TOUCHED` option. A pixel that a segment meets only at a single point is also selected when that point belongs to the pixel under the half-open rule used for points. Where a segment crosses a pixel corner exactly, the pixel after that corner in pixel order is selected. The first point of a segment selects the pixel containing it, and the last point selects nothing. The first point is the end with the smaller pixel `x` coordinate, or, for a segment that runs along a pixel column, the end with the smaller pixel `y` coordinate. Only the part of a segment inside the raster counts: a segment that meets the raster at a single point selects nothing, because that point is also its last. This is the rule of GDAL 3.11 and later. Earlier GDAL versions order the ends of a sloped segment that stays within one pixel column by row instead, and do not select the pixel containing its first point. GDAL also computes all of this in floating point with small tolerances, so its result can differ by a pixel for a segment that is within 0.01 pixel of horizontal or vertical, or that passes within rounding error of a pixel corner or a grid line.

For `LineString` geometries, `allTouched=true` can expand the geometry extent at grid boundaries. With `useGeometryExtent=true`, this can change both the output dimensions and the selected pixels. With `allTouched=false`, the extent is expanded by one pixel only past a grid line that a segment lies on, when that grid line is the last edge of the extent in pixel order, so that the pixels the segment selects are part of the output.

A line is rasterized wherever it crosses the raster, including along segments whose two vertices both lie outside the raster.

Whether a coordinate lies on a grid line is decided in decimal. Each stored coordinate, the raster's upper-left corner and its pixel size are read as the shortest decimal number that converts back to the same double, and the pixel position is computed exactly from those decimals. For example, on a raster with upper-left `y = 0.6` and `ScaleY = -0.1`, the coordinate `0.3` is on the grid line three pixels down, although `(0.3 - 0.6) / -0.1` evaluates to `2.9999999999999996` in floating point. A coordinate next to a grid line is not moved onto it: `0.30000000000000004`, the next double above `0.3` and also the result of `0.1 + 2 * 0.1`, lies past that line, so a line ending there enters the next pixel. Coordinates computed in floating point from the raster's origin and pixel size are therefore on a grid line only if their decimal form is. This rule applies to points, to lines, to polygon outlines under `allTouched=true`, and to the geometry extent.

The output is on the grid of `raster`. It has the same pixel size. With `useGeometryExtent=false` it has the georeference of `raster`, so a geometry selects the same pixels on the output as on `raster`, for example when the output is passed to `RS_ZonalStats` with the same geometry. With `useGeometryExtent=true` the output starts a whole number of pixels into that grid, and its origin is the grid line it starts on, rounded to a double.

When the upper-left corner and the pixel size of `raster` are short decimals, such as `0.6` and `0.1`, its grid lines are doubles, and the grid lines of a cropped output are exactly the same. For most rasters read from files they are not: with a pixel size of one arc second, `0.0002777777777777778`, no grid line is a double. The origin of a cropped output is then the double next to the grid line, less than a unit in the last place away from it, and so are its other grid lines. A coordinate that close to a grid line, such as a pixel corner computed from the raster's origin and pixel size, can select the neighbouring pixel on the cropped output. Use `useGeometryExtent=false` when the output is to be used as a reference raster for such coordinates.

A raster is stored with its cell-centre coordinates, and the corner it reports afterwards is computed from them in floating point, as `(corner + pixel size / 2) - pixel size / 2`. That is not always the corner the raster was made with: with a pixel size of `0.1`, the corner `0.1` becomes `0.10000000000000002`. A raster is stored when it passes from one step of a query to the next, but not between two functions nested in one expression. Pixels are selected on the grid of the stored corner in both cases, so a geometry selects the same pixels wherever in a query the raster was made. On the grid of that example the coordinate `0.3` is inside a pixel row and not on a grid line, because the grid lines are `0.10000000000000002 + k * 0.1`. A corner that does not change when it is stored, as when the corner and the pixel size are whole numbers, is used as it is.

With `useGeometryExtent=true` the output does not start on a grid line that would change when stored. It starts one pixel earlier, so it can have a first row or column that `geom` does not reach.

Format:

```
RS_AsRaster(geom: Geometry, raster: Raster, pixelType: String)
```

```
RS_AsRaster(geom: Geometry, raster: Raster, pixelType: String, allTouched: Boolean)
```

```
RS_AsRaster(geom: Geometry, raster: Raster, pixelType: String, allTouched: Boolean, value: Double)
```

```
RS_AsRaster(geom: Geometry, raster: Raster, pixelType: String, allTouched: Boolean, value: Double, noDataValue: Double)
```

```
RS_AsRaster(geom: Geometry, raster: Raster, pixelType: String, allTouched: Boolean, value: Double, noDataValue: Double, useGeometryExtent: Boolean)
```

Return type: `Raster`

Since: `v1.5.0`

!!!note
    The function doesn't support rasters that have any one of the following properties:
    ```
    ScaleX < 0
    SkewX != 0
    SkewY != 0
    ```
    If a raster is provided with any one of these properties, then an IllegalArgumentException is thrown.

For more information about ScaleX, ScaleY, SkewX, SkewY, please refer to the [Affine Transformations](../Raster-affine-transformation.md) section.

SQL Example

```sql
SELECT RS_AsRaster(
        ST_GeomFromWKT('POLYGON((15 15, 18 20, 15 24, 24 25, 15 15))'),
        RS_MakeEmptyRaster(2, 255, 255, 3, 30, 2, -2, 0, 0, 4326),
        'D', false, 255.0, 0d
    )
```

Output:

```
GridCoverage2D["g...
```

SQL Example

With the `noDataValue` omitted, the output inherits the reference band's no data value (here `0`, set on the empty reference raster) and fills uncovered pixels with it:

```sql
SELECT RS_AsRaster(
        ST_GeomFromWKT('POLYGON((15 15, 18 20, 15 24, 24 25, 15 15))'),
        RS_SetBandNoDataValue(RS_MakeEmptyRaster(2, 255, 255, 3, 30, 2, -2, 0, 0, 4326), 1, 0),
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
