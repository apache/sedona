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

# RS_GeoTransform

Introduction: Returns a struct of parameters that represent the GeoTransformation of the raster. The struct has the following schema:

- magnitudeI: size of a pixel along the transformed i axis
- magnitudeJ: size of a pixel along the transformed j axis
- thetaI: angle by which the raster is rotated (Radians positive clockwise)
- thetaIJ: angle from transformed i axis to transformed j axis (Radians positive counter-clockwise)
- offsetX: X ordinate of the upper-left corner of the upper-left pixel
- offsetY: Y ordinate of the upper-left corner of the upper-left pixel

!!!note
    Refer to [this image](https://www.researchgate.net/figure/Relation-between-the-cartesian-axes-x-y-and-i-j-axes-of-the-pixels_fig3_313860913) for a clear understanding between i & j axis and x & y-axis.

Format: `RS_GeoTransform(raster: Raster)`

Return type: `Struct<Double, Double, Double, Double, Double, Double>`

Since: `v1.5.1`

SQL Example

```sql
SELECT RS_GeoTransform(
        RS_MakeEmptyRaster(2, 10, 15, 1, 2, 1, -2, 1, 2, 0)
       )
```

Output:

```
{2.23606797749979, 2.23606797749979, -1.1071487177940904, -2.214297435588181, 1.0, 2.0}
```
