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

# ST_MakeLine

Introduction: Creates a geography LineString containing the coordinates of two Point, MultiPoint, or LineString geographies. Other geography types cause an error. When the result is passed to geography measurement functions, its edges are interpreted as great-circle arcs.

Format:

`ST_MakeLine(geog1: Geography, geog2: Geography)`

Return type: `Geography`

Since: `v1.9.1`

The output preserves the first input's SRID.

SQL Example

```sql
SELECT
  ST_AsText(line),
  ST_Length(line)
FROM (
  SELECT ST_MakeLine(
    ST_GeogFromWKT('POINT (0 0)', 4326),
    ST_GeogFromWKT('POINT (1 0)', 4326)
  ) AS line
);
```

Output:

```
LINESTRING (0 0, 1 0) | 111195.1
```
