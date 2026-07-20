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

# RS_FromNetCDF

Introduction: Returns a raster geometry representing the given record variable short name from a NetCDF file.
This API reads the array data of the record variable *in memory* along with all its dimensions
Since the netCDF format has many variants, the reader might not work for your test case, if that is so, please report this using the public forums.

This API has been tested for netCDF classic (NetCDF 1, 2, 5) and netCDF4/HDF5 files.

This API requires the name of the record variable. It is assumed that a variable of the given name exists, and its last 2 dimensions are 'lat' and 'lon' dimensions *respectively*.

If this assumption does not hold true for your case, you can choose to pass the lonDimensionName and latDimensionName explicitly.

You can use [RS_NetCDFInfo](RS_NetCDFInfo.md) to get the details of the passed netCDF file (variables and its dimensions).

## Coordinate and value handling

For a plain coordinate name, the loader searches the record variable's group and its
ancestors through the dimension's local apex, then searches downward from that apex
width-wise, level by level. A candidate must be a one-dimensional numeric variable over
the exact dimension used by the record variable. A coordinate supplied as a group path is
also validated against that dimension. This keeps sibling-group coordinates available
without confusing identically named dimensions in unrelated groups.

Coordinate, band-dimension, and raster values use the attributes of their owning variable.
Integer values declared with `_Unsigned` are widened, then `scale_factor` and `add_offset`
are applied. The output raster is normalized to west-to-east, north-up order: columns are
reversed for a decreasing X axis and rows are reversed for an increasing Y axis.

For raster values, `_FillValue`, every value in `missing_value`, and the limits declared by
`valid_range` (or `valid_min` and `valid_max`) are evaluated in the packed storage domain
before scale and offset are applied. Invalid cells become raster no-data. When the file's
declared value cannot safely represent no-data in the decoded raster — including packed,
64-bit integer, range-only, and non-finite missing-value cases — Sedona chooses a finite
decoded-domain sentinel that does not collide with a valid sample. Consequently,
`RS_BandNoDataValue` can intentionally differ from the file's `_FillValue` or
`missing_value` attribute.

Format 1: `RS_FromNetCDF(netCDF: ARRAY[Byte], recordVariableName: String)`

Format 2: `RS_FromNetCDF(netCDF: ARRAY[Byte], recordVariableName: String, lonDimensionName: String, latDimensionName: String)`

Return type: `Raster`

Since: `v1.5.1`

Spark Example:

```scala
val df = sedona.read.format("binaryFile").load("/some/path/test.nc")
df = df.withColumn("raster", f.expr("RS_FromNetCDF(content, 'O3')"))
```

```scala
val df = sedona.read.format("binaryFile").load("/some/path/test.nc")
df = df.withColumn("raster", f.expr("RS_FromNetCDF(content, 'O3', 'lon', 'lat')"))
```
