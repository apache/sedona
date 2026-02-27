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

# RS_AddBandFromArray

Introduction: Add a band to a raster from an array of doubles.

Format:

`RS_AddBandFromArray (raster: Raster, band: ARRAY[Double])`

`RS_AddBandFromArray (raster: Raster, band: ARRAY[Double], bandIndex: Integer)`

`RS_AddBandFromArray (raster: Raster, band: ARRAY[Double], bandIndex: Integer, noDataValue: Double)`

Since: `v1.5.0`

The bandIndex is 1-based and must be between 1 and RS_NumBands(raster) + 1. It throws an exception if the bandIndex is out of range or the raster is null. If not specified, the noDataValue of the band is assumed to be null.

When the bandIndex is RS_NumBands(raster) + 1, it appends the band to the end of the raster. Otherwise, it replaces the existing band at the bandIndex.

If the bandIndex and noDataValue is not given, a convenience implementation adds a new band with a null noDataValue.

Adding a new band with a custom noDataValue requires bandIndex = RS_NumBands(raster) + 1 and non-null noDataValue to be explicitly specified.

Modifying or Adding a customNoDataValue is also possible by giving an existing band in RS_AddBandFromArray

In order to remove an existing noDataValue from an existing band, pass null as the noDataValue in the RS_AddBandFromArray.

Note that: `bandIndex == RS_NumBands(raster) + 1` is an experimental feature and might lead to the loss of raster metadata and properties such as color models.

!!!Note
    RS_AddBandFromArray typecasts the double band values to the given datatype of the raster. This can lead to overflow values if values beyond the range of the raster's datatype are provided.

SQL Example

```sql
SELECT RS_AddBandFromArray(raster, RS_MultiplyFactor(RS_BandAsArray(RS_FromGeoTiff(content), 1), 2)) AS raster FROM raster_table
SELECT RS_AddBandFromArray(raster, RS_MultiplyFactor(RS_BandAsArray(RS_FromGeoTiff(content), 1), 2), 1) AS raster FROM raster_table
SELECT RS_AddBandFromArray(raster, RS_MultiplyFactor(RS_BandAsArray(RS_FromGeoTiff(content), 1), 2), 1, -999) AS raster FROM raster_table
```

Output:

```
+--------------------+
|              raster|
+--------------------+
|GridCoverage2D["g...|
+--------------------+
```
