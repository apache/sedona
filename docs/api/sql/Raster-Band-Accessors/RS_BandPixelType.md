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

# RS_BandPixelType

Introduction: Returns the datatype of each pixel in the given band of the given raster in string format. The band parameter is 1-indexed. If no band is specified, band 1 is assumed.
!!!Note
    If the given band index does not exist in the given raster, RS_BandPixelType throws an IllegalArgumentException.
Following are the possible values returned by RS_BandPixelType:

1. `REAL_64BITS` - For Double values
2. `REAL_32BITS` - For Float values
3. `SIGNED_32BITS` - For Integer values
4. `SIGNED_16BITS` - For Short values
5. `UNSIGNED_16BITS` - For unsigned Short values
6. `UNSIGNED_8BITS` - For Byte values

Format: `RS_BandPixelType(rast: Raster, band: Integer = 1)`

Since: `v1.5.0`

SQL Example

```sql
SELECT RS_BandPixelType(RS_MakeEmptyRaster(2, "D", 5, 5, 53, 51, 1, 1, 0, 0, 0), 2);
```

Output:

```
REAL_64BITS
```

```sql
SELECT RS_BandPixelType(RS_MakeEmptyRaster(2, "I", 5, 5, 53, 51, 1, 1, 0, 0, 0));
```

Output:

```
SIGNED_32BITS
```

```sql
SELECT RS_BandPixelType(RS_MakeEmptyRaster(2, "I", 5, 5, 53, 51, 1, 1, 0, 0, 0), 3);
```

Output:

```
IllegalArgumentException: Provided band index 3 is not present in the raster
```
