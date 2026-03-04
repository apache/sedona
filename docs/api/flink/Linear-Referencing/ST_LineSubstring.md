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

# ST_LineSubstring

Introduction: Return a linestring being a substring of the input one starting and ending at the given fractions of total 2d length. Second and third arguments are Double values between 0 and 1. This only works with LINESTRINGs.

Format: `ST_LineSubstring (geom: Geometry, startfraction: Double, endfraction: Double)`

Return type: `Geometry`

Since: `v1.5.0`

Example:

```sql
SELECT ST_LineSubstring(ST_GeomFromWKT('LINESTRING(25 50, 100 125, 150 190)'), 0.333, 0.666)
```

Output:

```
LINESTRING (69.28469348539744 94.28469348539744, 100 125, 111.70035626068274 140.21046313888758)
```
