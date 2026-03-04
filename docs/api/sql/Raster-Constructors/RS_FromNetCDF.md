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
