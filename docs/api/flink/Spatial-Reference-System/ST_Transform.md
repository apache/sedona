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

# ST_Transform

Introduction:

Transform the Spatial Reference System / Coordinate Reference System of A, from SourceCRS to TargetCRS.

Since `v1.9.0`, Sedona supports multiple CRS formats including EPSG codes, WKT1, WKT2, PROJ strings, and PROJJSON. Grid files for high-accuracy datum transformations are also supported.

!!!tip
    For comprehensive details on supported CRS formats, grid file usage, and examples, see the Spark SQL [CRS Transformation](../../sql/CRS-Transformation.md) documentation.

Format:

```
ST_Transform (A: Geometry, SourceCRS: String, TargetCRS: String)
```

Return type: `Geometry`

Since: `v1.2.0`

Example:

```sql
SELECT ST_AsText(ST_Transform(ST_GeomFromText('POLYGON((170 50,170 72,-130 72,-130 50,170 50))'),'EPSG:4326', 'EPSG:32649'))
```

Output:

```
POLYGON ((8766047.980342899 17809098.336766362, 5122546.516721856 18580261.912528664, 3240775.0740796793 -13688660.50985159, 4556241.924514083 -12463044.21488129, 8766047.980342899 17809098.336766362))
```
