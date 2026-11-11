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

# ST_H3ToCenterChild

Introduction: Returns the center child of an H3 cell at the requested resolution using H3's [cellToCenterChild(cell, resolution)](https://h3geo.org/docs/api/hierarchy#celltocenterchild).

The requested resolution must be between the input cell's resolution and 15, inclusive. An equal resolution returns the input cell. A lower resolution or a resolution outside this range raises an error. A null cell or resolution returns null.

The function returns a single center descendant at the target resolution, including when skipping multiple resolutions. For a pentagon input, the center child is also a pentagon.

Format: `ST_H3ToCenterChild(cell: Long, resolution: Int)`

Return type: `Long`

Since: `v2.0.0`

SQL Example

Find the resolution 6 center child of a resolution 5 hexagon:

```sql
SELECT ST_H3ToCenterChild(601041811137363967, 6)
```

Output:

```text
605545409825210367
```

An equal resolution retains the input cell:

```sql
SELECT ST_H3ToCenterChild(601041811137363967, 5)
```

Output:

```text
601041811137363967
```
