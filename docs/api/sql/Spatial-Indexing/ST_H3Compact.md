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

# ST_H3Compact

Introduction: Compacts an array of H3 cell IDs using H3's [compactCells(cells)](https://h3geo.org/docs/api/hierarchy#compactcells).

Complete sets of child cells are replaced by their parents recursively. Cells that cannot be compacted are retained. The result may contain cells at different resolutions and represents the same set of cells when expanded to the original resolution with `ST_H3Uncompact`.

Input cells must be unique, valid H3 cell IDs at the same resolution. H3 errors are propagated. An empty array returns an empty array, and a null array returns null. The order of the returned cells is not guaranteed.

Format: `ST_H3Compact(cells: Array<Long>)`

Return type: `Array<Long>`

Since: `v2.0.0`

SQL Example

Compact the 49 resolution 7 descendants of a resolution 5 hexagon back to their ancestor:

```sql
SELECT ST_H3Compact(ST_H3ToChildren(601041811137363967, 7))
```

Output:

```text
[601041811137363967]
```

A single cell is retained:

```sql
SELECT ST_H3Compact(array(601041811137363967))
```

Output:

```text
[601041811137363967]
```
