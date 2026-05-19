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

# RS_ScaleY

Introduction: Returns the pixel height of the raster in CRS units.

!!!Note
    RS_ScaleY attempts to get an Affine transform on the grid in order to return scaleX (See [World File](https://en.wikipedia.org/wiki/World_file) for more details). If the transform on the geometry is not an Affine transform, RS_ScaleY will throw an UnsupportedException:
    ```
    UnsupportedOperationException("Only AffineTransform2D is supported")
    ```

For more information about ScaleX, ScaleY, SkewX, SkewY, please refer to the [Affine Transformations](../Raster-affine-transformation.md) section.

![RS_ScaleY](../../../image/RS_ScaleY/RS_ScaleY.svg "RS_ScaleY")

Format: `RS_ScaleY(raster: Raster)`

Return type: `Double`

Since: `v1.5.0`

SQL Example

```sql
SELECT RS_ScaleY(raster) FROM rasters
```

Output:

```
-2
```
