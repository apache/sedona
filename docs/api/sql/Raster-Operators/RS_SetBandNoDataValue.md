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

# RS_SetBandNoDataValue

Introduction: This sets the no data value for a specified band in the raster. If the band index is not provided, band 1 is assumed by default. Passing a `null` value for `noDataValue` will remove the no data value and that will ensure all pixels are included in functions rather than excluded as no data.

Since `v1.5.1`, this function supports the ability to replace the current no-data value with the new `noDataValue`.

!!!Note
    When `replace` is true, any pixels matching the provided `noDataValue` will be considered as no-data in the output raster.

    An `IllegalArgumentException` will be thrown if the input raster does not already have a no-data value defined. Replacing existing values with `noDataValue` requires a defined no-data baseline to evaluate against.

    To use this for no-data replacement, the input raster must first set its no-data value, which can then be selectively replaced via this function.

Format:

```
RS_SetBandNoDataValue(raster: Raster, bandIndex: Integer, noDataValue: Double, replace: Boolean)
```

```
RS_SetBandNoDataValue(raster: Raster, bandIndex: Integer = 1, noDataValue: Double)
```

Since: `v1.5.0`

SQL Example

```sql
SELECT RS_BandNoDataValue(
        RS_SetBandNoDataValue(
            RS_MakeEmptyRaster(1, 20, 20, 2, 22, 2, 3, 1, 1, 0),
            -999
            )
        )
```

Output:

```
-999
```
