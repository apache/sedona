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

# RS_SRID

Introduction: Returns the spatial reference system identifier (SRID) of the raster geometry. Returns 0 if the raster has no CRS defined or if the CRS is a custom (non-EPSG) coordinate reference system. To retrieve the full CRS definition for custom CRS, use [RS_CRS](RS_CRS.md).

Format: `RS_SRID (raster: Raster)`

Return type: `Integer`

Since: `v1.4.1`

SQL Example

```sql
SELECT RS_SRID(raster) FROM raster_table
```

Output:

```
3857
```
