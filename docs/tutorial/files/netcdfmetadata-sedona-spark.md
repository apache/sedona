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

Load one or more directories, which are scanned recursively for all
`.nc`/`.nc4`/`.netcdf` files:

```python
df = sedona.read.format("netcdf.metadata").load("s3a://bucket/climate-data/")
```

Or use a glob pattern:

```python
df = sedona.read.format("netcdf.metadata").load("/path/to/*.nc")
```

For directory loads, `recursiveFileLookup=true` and a NetCDF extension
`pathGlobFilter` are applied as defaults; an explicit option always wins. In
particular, set `recursiveFileLookup=false` to keep Hive-style partition discovery
(e.g. `year=2020/` subdirectories become a `year` column):

```python
df = (
    sedona.read.format("netcdf.metadata")
    .option("recursiveFileLookup", "false")
    .load("/data/climate/")
)
```

The data source is read-only; writing is not supported, and neither are catalog
table operations (`CREATE TABLE ... USING netcdf.metadata`).

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
| `srid` | Int | EPSG code resolved from the CRS WKT (or inferred for a plain `latitude_longitude` grid mapping); null if not resolvable |
| `crs` | String | CRS in WKT form (WKT1 or WKT2, verbatim as declared by the file) from the CF `grid_mapping` variable (`crs_wkt`/`spatial_ref`) or the equivalent global attributes; null if absent |
| `geoTransform` | Struct | GDAL-style affine transform; null for irregular grids, and whenever `cornerCoordinates` is null (see below) |
| `cornerCoordinates` | Struct | Spatial extent (minX/minY/maxX/maxY); null if the file has no gridded variable, its trailing dimensions have no 1-D coordinate variables, or a coordinate variable has fewer than two finite values |
| `dimensions` | Array[Struct] | All dimensions in the file |
| `variables` | Array[Struct] | All variables in the file, including coordinate variables |
| `globalAttributes` | Map[String,String] | Global (root group) attributes |

The grid-defining variable's trailing two dimensions are interpreted as (Y, X), the
same convention as `RS_FromNetCDF`. Among variables with at least two dimensions,
variables referenced by CF metadata attributes (`bounds`, `climatology`,
`coordinates`, `ancillary_variables`, `cell_measures`, `formula_terms`) describe
other variables — cell boundaries like `lat_bnds`, quality flags, cell areas — and
are skipped; a variable whose trailing dimensions have matching 1-D numeric
coordinate variables is preferred. Coordinate variables are resolved in the data
variable's own group and its ancestors through the dimension's local apex first,
then downward from that apex width-wise, level by level. Candidates are matched
strictly by dimension identity, so identically named dimensions declared in
unrelated groups are never confused. `grid_mapping` references may be plain names
(resolved by proximity) or absolute/relative group paths (`/crs`, `../crs`).

Coordinate values are decoded before use: the CF packing attributes `scale_factor`
and `add_offset` are applied, and `_Unsigned` integer coordinates are widened, so
transforms and extents are always in real-world units rather than raw storage units.

### CRS resolution

The CRS is looked up in this order:

1. `crs_wkt` (CF) or `spatial_ref` (GDAL) on the `grid_mapping` variable of the
   grid-defining variable — both the simple (`"crs"`) and extended
   (`"crs: lat lon"`) forms of the `grid_mapping` attribute are understood. When
   the extended form declares several mappings (e.g.
   `"geographic: lat lon projected: x y"`), the mapping whose coordinate list
   matches the grid's (Y, X) coordinates is selected, so the reported CRS always
   corresponds to the coordinates used for the extent;
2. the same attribute names as global attributes.

The WKT is reported verbatim in `crs`, and `srid` is the EPSG identity of that WKT
(resolved with proj4sedona, so no GeoTools runtime is required). When the file
carries no WKT at all, `srid = 4326` (with a null `crs`) is reported only for a
`latitude_longitude` grid mapping that positively identifies the WGS 84 datum by
name (`horizontal_datum_name` or `geographic_crs_name`) with a Greenwich prime
meridian; ellipsoid parameters never qualify on their own (many datums share the
WGS 84 ellipsoid) and disable the inference when they contradict it. Projected grid
mappings defined only by CF parameters (e.g. `lambert_conformal_conic` without
`crs_wkt`) are not translated — both columns stay null.

### Using the CRS downstream

`RS_FromNetCDF` does not georeference the rasters it loads (their SRID is 0), so
the columns reported here are the practical way to attach a CRS:

```python
# Common case: carry the integer SRID
meta = sedona.read.format("netcdf.metadata").load(path).select("srid").first()
rast = file_df.selectExpr("RS_FromNetCDF(content, 'O3') as rast").selectExpr(
    f"RS_SetSRID(rast, {meta['srid']}) as rast"
)

# Full-fidelity case (non-EPSG or custom CRS): carry the WKT.
# RS_SetCRS accepts EPSG codes, WKT1, WKT2, PROJ strings, and PROJJSON.
meta = sedona.read.format("netcdf.metadata").load(path).select("crs").first()
rast = file_df.selectExpr("RS_FromNetCDF(content, 'O3') as rast").selectExpr(
    "RS_SetCRS(rast, '" + meta["crs"] + "') as rast"
)
```

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
| `dimensions` | Array[String] | Names of the variable's dimensions, in order, qualified with the declaring group's path (e.g. `sub/lat`) so they join to the `dimensions` column |
| `shape` | Array[Int] | Length of each dimension, in order |
| `units` | String | CF `units` attribute; null if absent |
| `longName` | String | CF `long_name` attribute; null if absent |
| `standardName` | String | CF `standard_name` attribute; null if absent |
| `noDataValue` | Double | First CF `_FillValue`, falling back to the first `missing_value`, in storage units after unsigned widening but without scale/offset; null if absent |
| `isCoordinate` | Boolean | Whether this is a coordinate variable (1-D, named after its dimension) |
| `attributes` | Map[String,String] | All attributes of the variable, verbatim |

The metadata source reports the no-data attribute declared by the file. The raster
loader evaluates all missing and validity attributes in storage units and may choose a
finite, collision-free decoded-domain sentinel for the loaded band. Therefore this
`noDataValue` can intentionally differ from `RS_BandNoDataValue` after
`RS_FromNetCDF`.

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
