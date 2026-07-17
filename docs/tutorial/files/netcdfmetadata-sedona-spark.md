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

# NetCdfMetadata - NetCDF File Metadata

The `netcdf.metadata` data source reads NetCDF file metadata — dimensions, variables,
attributes, and spatial extent — without loading data arrays into memory, similar to
running `ncdump -h` or [gdalinfo](https://gdal.org/en/stable/programs/gdalinfo.html)
at scale. It returns one row per file.

Only the file header is parsed. When the spatial extent (`geoTransform` or
`cornerCoordinates`) is requested, the 1-D coordinate variable arrays (latitude and
longitude values) are additionally read — never the data arrays themselves.

Supported file extensions are `.nc`, `.nc4`, and `.netcdf`. Both classic NetCDF
(CDF-1/2/5) and NetCDF-4 (HDF5-based) files are supported.

## Reading metadata from cloud storage

Files on object stores such as S3 are read with ranged requests: only the byte ranges
needed for the header (plus coordinate arrays, if the extent is requested) are
transferred, never the full file. The reader hints a random-access read policy to the
Hadoop filesystem (`fs.option.openfile.read.policy=random`), so S3A serves each read
as a small range request.

Classic NetCDF files store their header contiguously at the start of the file, so
metadata extraction needs only a few requests. NetCDF-4 (HDF5) files scatter their
metadata across the file, which results in more, smaller range requests — still only
kilobytes of transfer for multi-gigabyte files.

## Read NetCDF metadata

=== "Scala"

    ```scala
    val df = sedona.read.format("netcdf.metadata").load("/path/to/data.nc")
    df.show()
    ```

=== "Java"

    ```java
    Dataset<Row> df = sedona.read().format("netcdf.metadata").load("/path/to/data.nc");
    df.show();
    ```

=== "Python"

    ```python
    df = sedona.read.format("netcdf.metadata").load("/path/to/data.nc")
    df.show()
    ```

Load a directory, which is scanned recursively for all `.nc`/`.nc4`/`.netcdf` files:

```python
df = sedona.read.format("netcdf.metadata").load("s3a://bucket/climate-data/")
```

Or use a glob pattern:

```python
df = sedona.read.format("netcdf.metadata").load("/path/to/*.nc")
```

The data source is read-only; writing is not supported.

## Output schema

Each row describes one NetCDF file:

| Column | Type | Description |
|--------|------|-------------|
| `path` | String | File path |
| `driver` | String | Always `"NetCDF"` |
| `fileSize` | Long | File size in bytes |
| `format` | String | File type reported by the reader, e.g. `NetCDF` (classic) or `NetCDF-4` |
| `width` | Int | Grid X size (length of the X/longitude dimension); null if the file has no gridded (rank ≥ 2) variable |
| `height` | Int | Grid Y size (length of the Y/latitude dimension); null if the file has no gridded (rank ≥ 2) variable |
| `srid` | Int | EPSG code resolved from the CRS WKT; null if not resolvable |
| `crs` | String | CRS in WKT form from the CF `grid_mapping` variable (`crs_wkt`/`spatial_ref`) or the equivalent global attributes; null if absent |
| `geoTransform` | Struct | GDAL-style affine transform; null for irregular grids, and whenever `cornerCoordinates` is null (see below) |
| `cornerCoordinates` | Struct | Spatial extent (minX/minY/maxX/maxY); null if the file has no gridded variable, its trailing dimensions have no 1-D coordinate variables, or a coordinate variable has fewer than two finite values |
| `dimensions` | Array[Struct] | All dimensions in the file |
| `variables` | Array[Struct] | All variables in the file, including coordinate variables |
| `globalAttributes` | Map[String,String] | Global (root group) attributes |

The grid-defining variable follows the same convention as `RS_FromNetCDF`: the first
variable with at least two dimensions, whose trailing two dimensions are interpreted
as (Y, X).

### geoTransform struct

| Field | Type | Description |
|-------|------|-------------|
| `upperLeftX` | Double | X coordinate of the top-left corner of the top-left pixel |
| `upperLeftY` | Double | Y coordinate of the top-left corner of the top-left pixel |
| `scaleX` | Double | Pixel width |
| `scaleY` | Double | Pixel height (negative: north-up) |
| `skewX` | Double | Always 0 |
| `skewY` | Double | Always 0 |

CF coordinate values are pixel centers. For an evenly spaced grid, the transform
origin is placed half a pixel outside the first coordinate value (GDAL convention),
matching what `RS_FromNetCDF` reports. For an unevenly spaced grid an affine
transform would misrepresent the geometry, so `geoTransform` is null and
`cornerCoordinates` covers the coordinate centers only.

### cornerCoordinates struct

| Field | Type | Description |
|-------|------|-------------|
| `minX` | Double | Western edge |
| `minY` | Double | Southern edge |
| `maxX` | Double | Eastern edge |
| `maxY` | Double | Northern edge |

### dimensions array element

| Field | Type | Description |
|-------|------|-------------|
| `name` | String | Dimension name; dimensions in nested groups are prefixed with the group path |
| `length` | Int | Dimension length |
| `isUnlimited` | Boolean | Whether the dimension is unlimited (record dimension) |

### variables array element

| Field | Type | Description |
|-------|------|-------------|
| `name` | String | Variable name (full path for variables in nested groups) |
| `dataType` | String | NetCDF data type, e.g. `float`, `double`, `int` |
| `dimensions` | Array[String] | Names of the variable's dimensions, in order |
| `shape` | Array[Int] | Length of each dimension, in order |
| `units` | String | CF `units` attribute; null if absent |
| `longName` | String | CF `long_name` attribute; null if absent |
| `standardName` | String | CF `standard_name` attribute; null if absent |
| `noDataValue` | Double | CF `_FillValue`, falling back to `missing_value`; null if absent |
| `isCoordinate` | Boolean | Whether this is a coordinate variable (1-D, named after its dimension) |
| `attributes` | Map[String,String] | All attributes of the variable, verbatim |

## Examples

### Discover the variables in a collection of files

```python
sedona.read.format("netcdf.metadata").load("s3a://bucket/climate/*.nc").selectExpr(
    "path", "explode(variables) as v"
).where("NOT v.isCoordinate").selectExpr(
    "path", "v.name", "v.dataType", "v.shape", "v.units", "v.longName"
).show(
    truncate=False
)
```

The variable names discovered this way can be passed to `RS_FromNetCDF` to load a
specific variable as a raster:

```python
file_df = sedona.read.format("binaryFile").load("s3a://bucket/climate/2024-01.nc")
raster_df = file_df.selectExpr("RS_FromNetCDF(content, 'O3') as raster")
```

### Filter files by spatial extent

```python
sedona.read.format("netcdf.metadata").load("/data/netcdf/").where(
    "cornerCoordinates.minX <= 15 AND cornerCoordinates.maxX >= 5 "
    "AND cornerCoordinates.minY <= 55 AND cornerCoordinates.maxY >= 45"
).select("path", "width", "height").show()
```

Files whose spatial extent cannot be computed (no gridded variable, or no 1-D
coordinate variables for the grid dimensions) have a null `cornerCoordinates` and are
therefore excluded by a predicate like the one above.

### Inspect dimensions and global attributes

```python
sedona.read.format("netcdf.metadata").load("/data/netcdf/test.nc").selectExpr(
    "explode(dimensions) as d"
).selectExpr("d.name", "d.length", "d.isUnlimited").show()

sedona.read.format("netcdf.metadata").load("/data/netcdf/test.nc").select(
    "globalAttributes"
).show(truncate=False)
```

### Select specific columns

Column pruning reduces I/O: `path`, `driver`, and `fileSize` are served from the file
listing without opening the file at all, and the coordinate arrays are only read when
`geoTransform` or `cornerCoordinates` is selected.

```python
sedona.read.format("netcdf.metadata").load("/data/netcdf/").select(
    "path", "width", "height"
).show()
```
