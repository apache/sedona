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

# Migrating to Sedona 2.0

Sedona 2.0 is a major release. Most code runs unchanged, but a few behaviours change on
purpose: Sedona now matches PostGIS in places where it used to differ, and some inputs that
were silently mishandled now raise an error instead. This page walks through what to check
when you upgrade from 1.x. The [release notes](release-notes.md#sedona-200) list every change.

## Check your platform

* **Spark 3.5 or newer.** Spark 3.4 is no longer supported, and no `sedona-spark-3.4_*`
  artifacts are published for 2.0. Supported versions are Spark 3.5, 4.0 and 4.1. The Python
  package requires `pyspark>=3.5.0`.
* **Flink 1.19 or newer.** Flink 1.12 to 1.18 are no longer supported. Flink 2.2 is supported.
  Stateful Flink jobs that hold an `ST_MinimumBoundingRadius` result in their state can't
  restore from a checkpoint or savepoint taken with 1.x. Restart them without state.

## Element and pixel positions now count from 1

Four functions used to count positions from 0 and now count from 1, as PostGIS does. This is
the change most likely to affect existing queries, because the old queries still run: they
now return the *next* element or pixel, or `NULL` past the end, rather than raising an error.

| Function | Sedona 1.x | Sedona 2.0 (= PostGIS) |
|---|---|---|
| `ST_GeometryN(geom, n)` | `n = 0` is the first geometry | `n = 1` is the first geometry |
| `ST_InteriorRingN(polygon, n)` | `n = 0` is the first hole | `n = 1` is the first hole |
| `RS_Value(raster, colX, rowY, band)` | `(0, 0)` is the upper-left pixel | `(1, 1)` is the upper-left pixel |
| `RS_Values(raster, xs, ys, band)` | `(0, 0)` is the upper-left pixel | `(1, 1)` is the upper-left pixel |

In 2.0, an index of `0`, a negative index, or one past the last element returns `NULL`.

**What to change:** add 1 to the index or grid coordinate in every call to these four
functions, whether it's a literal or a column. For example:

```sql
-- Sedona 1.x
SELECT ST_GeometryN(geom, 0), RS_Value(rast, 3, 4, 1) FROM t
-- Sedona 2.0
SELECT ST_GeometryN(geom, 1), RS_Value(rast, 4, 5, 1) FROM t
```

Code that builds the index with `sequence(0, n - 1)` or a `range(0, n)` loop needs
`sequence(1, n)` or `range(1, n + 1)` instead.

**What doesn't change:**

* The point-geometry forms `RS_Value(raster, point[, band])` and `RS_Values(raster, points[, band])`.
* `RS_PixelAsPoint`, `RS_PixelAsCentroid`, `RS_PixelAsPolygon`, `RS_SetValue`, `RS_SetValues`,
  `RS_RasterToWorldCoord` and `RS_WorldToRasterCoord`, which were already 1-based, as were all
  band arguments and `ST_PointN`.
* `ST_AddPoint`, `ST_SetPoint` and `ST_RemovePoint`, which stay 0-based because PostGIS is
  0-based for these.
* The GeoPandas API: `GeoSeries.get_geometry` and `GeoSeries.interiors` keep GeoPandas'
  0-based indexing and handle the change internally.

The Python `ST_GeometryN` and `ST_InteriorRingN` DataFrame functions now reject an integer
`n` below 1 rather than below 0.

## Renamed functions

`RS_Union` and `RS_Union_Aggr` are now `RS_Stack` and `RS_Stack_Aggr`. They stack the bands of
several rasters into one raster, and never merged grids the way the old names suggested. The old
names are removed, so queries that use them fail until renamed; the behaviour is unchanged.

## Replacing a band's no-data value is a separate function

The four-argument `RS_SetBandNoDataValue(raster, band, noDataValue, replace)` form is removed,
so queries that use it fail until rewritten:

* `RS_SetBandNoDataValue(raster, band, value, true)` becomes
  `RS_ReplaceBandNoDataValue(raster, band, value)`, which rewrites the pixels holding the old
  no-data value to the new one before declaring it.
* `RS_SetBandNoDataValue(raster, band, value, false)` becomes
  `RS_SetBandNoDataValue(raster, band, value)`, which declares the new value and leaves the pixels
  as they are.

## Other behaviour changes

* **WKB output keeps M.** `ST_AsBinary`, `ST_AsEWKB` and `ST_AsHEXEWKB` now write the M
  ordinate of measured geometries, so these inputs produce XYM or XYZM rather than XY or XYZ.
* **`ST_IsPolygonCW` and `ST_IsPolygonCCW`** look inside nested `GeometryCollection` values and
  return `true` for inputs with no polygons, as PostGIS does. In Flink, a `NULL` input now returns
  `NULL`.
* **Mixed coordinate layouts are rejected.** Serializing a polygon or multi-geometry whose parts
  mix XY, XYZ, XYM or XYZM (for example from `ST_Collect`) now raises an error instead of
  corrupting ordinates. Normalize the parts to one layout, or use a `GeometryCollection`.
* **`RS_Resample` and `RS_ReprojectMatch`** raise an error for an unknown algorithm name.
  Previously they silently fell back to nearest neighbour.
* **`RS_SetBandNoDataValue(raster, band, NULL)`** now removes that band's no-data value from
  Spark SQL, as documented, and `RS_AsGeoTiff` no longer writes a no-data value of `0` for a
  raster that has none.
* **GeoPandas `set_crs`** defaults to `allow_override=False`, as GeoPandas does, so replacing an
  existing CRS needs `allow_override=True`.
* **GeoPandas `fillna`** validates a replacement Series' index when Spark evaluates the result,
  so a bad index raises a Spark error instead of an immediate `ValueError`.

## Deprecations

* **`ST_Force_2D`** is deprecated in favour of `ST_Force2D`, the name PostGIS uses. The old name
  still works in 2.0 but logs a warning.
