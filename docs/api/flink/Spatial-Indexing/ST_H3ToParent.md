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

# ST_H3ToParent

Introduction: Returns the result of the H3 function [cellToParent(cell, resolution)](https://h3geo.org/docs/api/hierarchy#celltoparent). The requested resolution must be between 0 and the input cell's resolution, inclusive.

Format: `ST_H3ToParent(cell: Long, resolution: Int)`

Return type: `Long`

Since: `v2.0.0`

Example:

```sql
SELECT ST_H3ToParent(614552609325318143, 5)
```

Output:

```
+----+--------------------+
| op |             EXPR$0 |
+----+--------------------+
| +I | 601041811137363967 |
+----+--------------------+
```
