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

# RS_AsBase64

Introduction: Returns a base64 encoded string of the given raster. If the datatype is integral then this function internally takes the first 4 bands as RGBA, and converts them to the PNG format, finally produces a base64 string. When the datatype is not integral, the function converts the raster to TIFF format, and then generates a base64 string. To visualize other bands, please use it together with `RS_Band`. You can take the resulting base64 string in [an online viewer](https://base64-viewer.onrender.com/) to check how the image looks like.

!!!Warning
    This is not recommended for large files.

Format:

`RS_AsBase64(raster: Raster, maxWidth: Integer)`

`RS_AsBase64(raster: Raster)`

Return type: `String`

Since: `v1.5.0`

SQL Example

```sql
SELECT RS_AsBase64(raster) from rasters
```

Output:

```
iVBORw0KGgoAAAA...
```
