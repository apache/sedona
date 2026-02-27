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

# RS_BandNoDataValue

Introduction: Returns the no data value of the given band of the given raster. If no band is given, band 1 is assumed. The band parameter is 1-indexed. If there is no data value associated with the given band, RS_BandNoDataValue returns null.

!!!Note
    If the given band does not lie in the raster, RS_BandNoDataValue throws an IllegalArgumentException

Format: `RS_BandNoDataValue (raster: Raster, band: Integer = 1)`

Since: `v1.5.0`

SQL Example

```sql
SELECT RS_BandNoDataValue(raster, 1) from rasters;
```

Output:

```
0.0
```

SQL Example

```sql
SELECT RS_BandNoDataValue(raster) from rasters_without_nodata;
```

Output:

```
null
```

SQL Example

```sql
SELECT RS_BandNoDataValue(raster, 3) from rasters;
```

Output:

```
IllegalArgumentException: Provided band index 3 is not present in the raster.
```
