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

# ST_H3Uncompact

Introduction: Expands an array of H3 cell IDs to the requested resolution using H3's [uncompactCells(cells, resolution)](https://h3geo.org/docs/api/hierarchy#uncompactcells).

The input cells may have different resolutions and do not need to come from a compaction operation. Each coarser cell is replaced by its descendants at the target resolution. Cells already at the target resolution are retained.

The requested resolution must be between 0 and 15, inclusive, and at least as fine as every input cell's resolution. A lower resolution or a resolution outside this range raises an error. An empty input array returns an empty array. A null array or resolution returns null.

Input cells should be unique and non-overlapping; duplicate or overlapping cells may produce duplicates in the output. The order of the returned cells is not guaranteed. The result can grow rapidly when expanding to a much finer resolution.

Format: `ST_H3Uncompact(cells: Array<Long>, resolution: Int)`

Return type: `Array<Long>`

Since: `v2.0.0`

SQL Example

```sql
SELECT ST_H3Uncompact(array(601041811137363967), 5)
```

Output:

```text
[601041811137363967]
```

To expand this resolution 5 hexagon to its seven immediate children:

```sql
SELECT SIZE(ST_H3Uncompact(array(601041811137363967), 6)) AS cell_count
```

Output:

```text
7
```
