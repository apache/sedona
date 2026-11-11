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

# ST_H3ToChildren

Introduction: Returns the children of an H3 cell at the requested resolution using H3's [cellToChildren(cell, resolution)](https://h3geo.org/docs/api/hierarchy#celltochildren).

The requested resolution must be between the input cell's resolution and 15, inclusive. An equal resolution returns an array containing only the input cell. A lower resolution or a resolution outside this range raises an error. A null cell or resolution returns null.

The result contains the cell's descendants in the H3 hierarchy. An immediate hexagon parent has seven children, while an immediate pentagon parent has six. The order of the returned cells is not guaranteed.

Format: `ST_H3ToChildren(cell: Long, resolution: Int)`

Return type: `Array<Long>`

Since: `v2.0.0`

SQL Example

```sql
SELECT ST_H3ToChildren(601041811137363967, 5)
```

Output:

```text
[601041811137363967]
```

To count the immediate children of this resolution 5 hexagon:

```sql
SELECT CARDINALITY(ST_H3ToChildren(601041811137363967, 6)) AS child_count
```

Output:

```text
7
```
