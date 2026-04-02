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

# SedonaInfo - Raster File Metadata

SedonaInfo is a Spark data source that reads raster file metadata without decoding pixel data, similar to [gdalinfo](https://gdal.org/en/stable/programs/gdalinfo.html). It returns one row per file with metadata including dimensions, coordinate system, band information, tiling, overviews, and compression.

This is useful for:

* Cataloging and inventorying large collections of raster files
* Detecting Cloud Optimized GeoTIFFs (COGs) by checking tiling and overview status
* Inspecting file properties before loading full raster data
* Building spatial indexes over raster file collections

Currently supports **GeoTIFF** files. Additional formats can be added in the future.

## Read GeoTIFF metadata

=== "Scala"

    ```scala
    val df = sedona.read.format("sedonainfo").load("/path/to/rasters/")
    df.show()
    ```

=== "Java"

    ```java
    Dataset<Row> df = sedona.read().format("sedonainfo").load("/path/to/rasters/");
    df.show();
    ```

=== "Python"

    ```python
    df = sedona.read.format("sedonainfo").load("/path/to/rasters/")
    df.show()
    ```

You can also use glob patterns:

```python
df = sedona.read.format("sedonainfo").load("/path/to/rasters/*.tif")
```

Or load a single file:

```python
df = sedona.read.format("sedonainfo").load("/path/to/image.tiff")
```

## Output schema

Each row represents one raster file with the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `path` | String | File path |
| `driver` | String | Format driver (e.g., "GTiff") |
| `fileSize` | Long | File size in bytes |
| `width` | Int | Image width in pixels |
| `height` | Int | Image height in pixels |
| `numBands` | Int | Number of bands |
| `srid` | Int | EPSG code (0 if unknown) |
| `crs` | String | Coordinate Reference System as WKT |
| `geoTransform` | Struct | Affine transform parameters |
| `cornerCoordinates` | Struct | Bounding box |
| `bands` | Array[Struct] | Per-band metadata |
| `overviews` | Array[Struct] | Overview (pyramid) levels |
| `metadata` | Map[String, String] | File-wide TIFF metadata tags |
| `isTiled` | Boolean | Whether the file uses internal tiling |
| `compression` | String | Compression type (e.g., "Deflate") |

### geoTransform struct

| Field | Type | Description |
|-------|------|-------------|
| `upperLeftX` | Double | Origin X in world coordinates |
| `upperLeftY` | Double | Origin Y in world coordinates |
| `scaleX` | Double | Pixel size in X direction |
| `scaleY` | Double | Pixel size in Y direction |
| `skewX` | Double | Rotation/shear in X |
| `skewY` | Double | Rotation/shear in Y |

### cornerCoordinates struct

| Field | Type | Description |
|-------|------|-------------|
| `minX` | Double | Minimum X (west) |
| `minY` | Double | Minimum Y (south) |
| `maxX` | Double | Maximum X (east) |
| `maxY` | Double | Maximum Y (north) |

### bands array element

| Field | Type | Description |
|-------|------|-------------|
| `band` | Int | Band number (1-indexed) |
| `dataType` | String | Data type (e.g., "REAL_32BITS") |
| `colorInterpretation` | String | Color interpretation (e.g., "Gray") |
| `noDataValue` | Double | NoData value (null if not set) |
| `blockWidth` | Int | Internal tile/block width |
| `blockHeight` | Int | Internal tile/block height |
| `description` | String | Band description |
| `unit` | String | Unit type (e.g., "meters") |

### overviews array element

| Field | Type | Description |
|-------|------|-------------|
| `level` | Int | Overview level (1, 2, 3, ...) |
| `width` | Int | Overview width in pixels |
| `height` | Int | Overview height in pixels |

## Examples

### Detect Cloud Optimized GeoTIFFs (COGs)

A COG is a GeoTIFF that is internally tiled and has overview levels:

```python
df = sedona.read.format("sedonainfo").load("/path/to/rasters/")
cogs = df.filter("isTiled AND size(overviews) > 0")
cogs.select("path", "compression", "overviews").show(truncate=False)
```

### Inspect band information

```python
df = sedona.read.format("sedonainfo").load("/path/to/image.tif")
df.selectExpr("path", "explode(bands) as band").selectExpr(
    "path",
    "band.band",
    "band.dataType",
    "band.noDataValue",
    "band.blockWidth",
    "band.blockHeight",
).show()
```

### Filter by spatial extent

```python
df = sedona.read.format("sedonainfo").load("/path/to/rasters/")
df.filter("cornerCoordinates.minX > -120 AND cornerCoordinates.maxX < -100").select(
    "path", "width", "height", "srid"
).show()
```

### Get overview details

```python
df = sedona.read.format("sedonainfo").load("/path/to/image.tif")
df.selectExpr("path", "explode(overviews) as ovr").selectExpr(
    "path", "ovr.level", "ovr.width", "ovr.height"
).show()
```

### Column pruning for performance

Select only the columns you need. SedonaInfo uses column pruning to skip extracting unused metadata:

```python
df = (
    sedona.read.format("sedonainfo")
    .load("/path/to/rasters/")
    .select("path", "width", "height", "numBands")
)
df.show()
```

## Supported formats

| Format | Driver | Extensions |
|--------|--------|------------|
| GeoTIFF | GTiff | `.tif`, `.tiff` |

Additional formats may be added in future releases.
