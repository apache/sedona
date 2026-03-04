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

# ST_BingTileToGeom

Introduction: Returns an array of Polygons for the corresponding Bing Tile quadkeys.

!!!Hint
    To convert a Polygon array to a single geometry, use [ST_Collect](../Geometry-Editors/ST_Collect.md).

Format: `ST_BingTileToGeom(quadKeys: Array[String])`

Return type: `Array<Geometry>`

Since: `v1.9.0`

SQL Example

```sql
SELECT ST_BingTileToGeom(array('0', '1', '2', '3'))
```

Output:

```
[POLYGON ((-180 85.05112877980659, -180 0, 0 0, 0 85.05112877980659, -180 85.05112877980659)), ...]
```
