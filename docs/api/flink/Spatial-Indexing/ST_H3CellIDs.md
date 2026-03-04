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

# ST_H3CellIDs

Introduction: Cover the geometry by H3 cell IDs with the given resolution(level).
To understand the cell statistics please refer to [H3 Doc](https://h3geo.org/docs/core-library/restable)
H3 native fill functions doesn't guarantee full coverage on the shapes.

## Cover Polygon

When fullCover = false, for polygon sedona will use [polygonToCells](https://h3geo.org/docs/api/regions#polygontocells).
This can't guarantee full coverage but will guarantee no false positive.

When fullCover = true, sedona will add on extra traversal logic to guarantee full coverage on shapes.
This will lead to redundancy but can guarantee full coverage.

Choose the option according to your use case.

## Cover LineString

For the lineString, sedona will call gridPathCells(https://h3geo.org/docs/api/traversal#gridpathcells) per segment.
From H3's documentation
> This function may fail to find the line between two indexes, for example if they are very far apart. It may also fail when finding distances for indexes on opposite sides of a pentagon.

When the `gridPathCells` function throw error, Sedona implemented in-house approximate implementation to generate the shortest path, which can cover the corner cases.

Both functions can't guarantee full coverage. When the `fullCover = true`, we'll do extra cell traversal to guarantee full cover.
In worst case, sedona will use MBR to guarantee the full coverage.

If you seek to get the shortest path between cells, you can call this function with `fullCover = false`

Format: `ST_H3CellIDs(geom: geometry, level: Int, fullCover: true)`

Return type: `Array<Long>`

Since: `v1.5.0`

Example:

```sql
SELECT ST_H3CellIDs(ST_GeomFromText('LINESTRING(1 3 4, 5 6 7)'), 6, true)
```

Output:

```
+----+--------------------------------+
| op |                         EXPR$0 |
+----+--------------------------------+
| +I | [605547539457900543, 605547... |
+----+--------------------------------+
```
