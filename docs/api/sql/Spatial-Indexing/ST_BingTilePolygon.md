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

# ST_BingTilePolygon

Introduction: Returns the bounding polygon (Geometry) of the Bing Tile identified by the given quadkey.

Format: `ST_BingTilePolygon(quadKey: String)`

Return type: `Geometry`

Since: `v1.9.0`

SQL Example

```sql
SELECT ST_AsText(ST_BingTilePolygon('213'))
```

Output:

```
POLYGON ((0 0, 0 -40.97989806962013, 45 -40.97989806962013, 45 0, 0 0))
```

![ST_BingTilePolygon](../../../image/ST_BingTilePolygon/ST_BingTilePolygon.svg)
