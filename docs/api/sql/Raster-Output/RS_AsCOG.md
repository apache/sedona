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

# RS_AsCOG

Introduction: Returns a binary DataFrame from a Raster DataFrame. Each raster object in the resulting DataFrame is a [Cloud Optimized GeoTIFF](https://www.cogeo.org/) (COG) image in binary format. COG is a GeoTIFF that is internally organized to enable efficient range-read access over HTTP, making it ideal for cloud-hosted raster data.

Possible values for `compression`: `Deflate` (default), `LZW`, `JPEG`, `PackBits`. Case-insensitive.

`tileSize` must be a power of 2 (e.g., 128, 256, 512). Default value: `256`

Possible values for `quality`: any decimal number between 0 and 1. 0 means maximum compression and 1 means minimum compression. Default value: `0.2`

Possible values for `resampling`: `Nearest` (default), `Bilinear`, `Bicubic`. Case-insensitive. This controls the resampling algorithm used to build overview levels.

`overviewCount` controls the number of overview levels. Use `-1` for automatic (default), `0` for no overviews, or any positive integer for a specific count.

Format:

`RS_AsCOG(raster: Raster)`

`RS_AsCOG(raster: Raster, compression: String)`

`RS_AsCOG(raster: Raster, compression: String, tileSize: Integer)`

`RS_AsCOG(raster: Raster, compression: String, tileSize: Integer, quality: Double)`

`RS_AsCOG(raster: Raster, compression: String, tileSize: Integer, quality: Double, resampling: String)`

`RS_AsCOG(raster: Raster, compression: String, tileSize: Integer, quality: Double, resampling: String, overviewCount: Integer)`

Return type: `Binary`

Since: `v1.9.0`

SQL Example

```sql
SELECT RS_AsCOG(raster) FROM my_raster_table
```

SQL Example

```sql
SELECT RS_AsCOG(raster, 'LZW') FROM my_raster_table
```

SQL Example

```sql
SELECT RS_AsCOG(raster, 'LZW', 512, 0.75, 'Bilinear', 3) FROM my_raster_table
```

Output:

```html
+--------------------+
|                 cog|
+--------------------+
|[4D 4D 00 2A 00 0...|
+--------------------+
```

Output schema:

```sql
root
 |-- cog: binary (nullable = true)
```
