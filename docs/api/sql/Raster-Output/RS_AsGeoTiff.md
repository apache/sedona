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

# RS_AsGeoTiff

Introduction: Returns a binary DataFrame from a Raster DataFrame. Each raster object in the resulting DataFrame is a GeoTiff image in binary format.

Possible values for `compressionType`: `None`, `PackBits`, `Deflate`, `Huffman`, `LZW` and `JPEG`

Possible values for `imageQuality`: any decimal number between 0 and 1. 0 means the lowest quality and 1 means the highest quality.

Format:

`RS_AsGeoTiff(raster: Raster)`

`RS_AsGeoTiff(raster: Raster, compressionType: String, imageQuality: Double)`

Return type: `Binary`

Since: `v1.4.1`

SQL Example

```sql
SELECT RS_AsGeoTiff(raster) FROM my_raster_table
```

SQL Example

```sql
SELECT RS_AsGeoTiff(raster, 'LZW', 0.75) FROM my_raster_table
```

Output:

```html
+--------------------+
|             geotiff|
+--------------------+
|[4D 4D 00 2A 00 0...|
+--------------------+
```

Output schema:

```sql
root
 |-- geotiff: binary (nullable = true)
```
