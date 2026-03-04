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

# RS_ConvexHull

Introduction: Return the convex hull geometry of the raster including the NoDataBandValue band pixels.
For regular shaped and non-skewed rasters, this gives more or less the same result as RS_Envelope and hence is only useful for irregularly shaped or skewed rasters.

Format: `RS_ConvexHull(raster: Raster)`

Return type: `Geometry`

Since: `v1.5.0`

SQL Example

```sql
SELECT RS_ConvexHull(RS_MakeEmptyRaster(1, 5, 10, 156, -132, 5, 10, 3, 5, 0));
```

Output:

```
POLYGON ((156 -132, 181 -107, 211 -7, 186 -32, 156 -132))
```
