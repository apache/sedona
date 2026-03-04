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

# RS_NormalizeAll

Introduction: Normalizes values in all bands of a raster between a given normalization range. The function maintains the data type of the raster values by ensuring that the normalized values are cast back to the original data type of each band in the raster. By default, the values are normalized to range [0, 255]. RS_NormalizeAll can take upto 7 of the following arguments.

- `raster`: The raster to be normalized.
- `minLim` and `maxLim` (Optional): The lower and upper limits of the normalization range. By default, normalization range is set to [0, 255].
- `normalizeAcrossBands` (Optional): A boolean flag to determine the normalization method. If set to true (default), normalization is performed across all bands based on global min and max values. If false, each band is normalized individually based on its own min and max values.
- `noDataValue` (Optional): Defines the value to be used for missing or invalid data in raster bands. By default, noDataValue is set to `maxLim` and Safety mode is triggered.
- `minValue` and `maxValue` (Optional): Optionally, specific minimum and maximum values of the input raster can be provided. If not provided, these values are computed from the raster data.

A Safety mode is triggered when `noDataValue` is not given. This sets `noDataValue` to `maxLim` and normalizes valid data values to the range [minLim, maxLim-1]. This is to avoid replacing valid data that might coincide with the new `noDataValue`.

!!! Warning
    Using a noDataValue that falls within the normalization range can lead to loss of valid data. If any data value within a raster band matches the specified noDataValue, it will be replaced and cannot be distinguished or recovered later. Exercise caution in selecting a noDataValue to avoid unintentional data alteration.

Formats:

```
RS_NormalizeAll (raster: Raster)
```

```
RS_NormalizeAll (raster: Raster, minLim: Double, maxLim: Double)
```

```
RS_NormalizeAll (raster: Raster, minLim: Double, maxLim: Double, normalizeAcrossBands: Boolean)
```

```
RS_NormalizeAll (raster: Raster, minLim: Double, maxLim: Double, normalizeAcrossBands: Boolean, noDataValue: Double)
```

```
RS_NormalizeAll (raster: Raster, minLim: Double, maxLim: Double, noDataValue: Double, minValue: Double, maxValue: Double)
```

```
RS_NormalizeAll (raster: Raster, minLim: Double, maxLim: Double, normalizeAcrossBands: Boolean, noDataValue: Double, minValue: Double, maxValue: Double )
```

Return type: `Raster`

Since: `v1.6.0`

SQL Example

```sql
SELECT RS_NormalizeAll(raster, 0, 1)
```
