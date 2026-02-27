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

# ST_S2ToGeom

Introduction: Returns an array of Polygons for the corresponding S2 cell IDs.

!!!Hint
    To convert a Polygon array to MultiPolygon, use [ST_Collect](../Geometry-Editors/ST_Collect.md). However, the result may be an invalid geometry. Apply [ST_MakeValid](../Geometry-Validation/ST_MakeValid.md) to the `ST_Collect` output to ensure a valid MultiPolygon.

    An alternative approach to consolidate a Polygon array into a Polygon/MultiPolygon, use the [ST_Union](../Overlay-Functions/ST_Union.md) function.

Format: `ST_S2ToGeom(cellIds: Array[Long])`

Since: `v1.6.0`

SQL Example:

```sql
SELECT ST_S2ToGeom(array(11540474045136890))
```

Output:

```
[POLYGON ((-36.609392788630245 -38.169532607255846, -36.609392706252954 -38.169532607255846, -36.609392706252954 -38.169532507473015, -36.609392788630245 -38.169532507473015, -36.609392788630245 -38.169532607255846))]
```
