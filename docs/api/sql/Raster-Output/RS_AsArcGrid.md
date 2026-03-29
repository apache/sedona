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

# RS_AsArcGrid

Introduction: Returns a binary DataFrame from a Raster DataFrame. Each raster object in the resulting DataFrame is an ArcGrid image in binary format. ArcGrid only takes 1 source band. If your raster has multiple bands, you need to specify which band you want to use as the source.

Possible values for `sourceBand`: any non-negative value (>=0). If not given, it will use Band 0.

Format:

`RS_AsArcGrid(raster: Raster)`

`RS_AsArcGrid(raster: Raster, sourceBand: Integer)`

Return type: `Binary`

Since: `v1.4.1`

SQL Example

```sql
SELECT RS_AsArcGrid(raster) FROM my_raster_table
```

SQL Example

```sql
SELECT RS_AsArcGrid(raster, 1) FROM my_raster_table
```

Output:

```html
+--------------------+
|             arcgrid|
+--------------------+
|[4D 4D 00 2A 00 0...|
+--------------------+
```

Output schema:

```sql
root
 |-- arcgrid: binary (nullable = true)
```
