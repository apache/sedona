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

# ST_Centroid

Introduction: Returns the planar centroid of a geography object as a Geography point. Note that this computes the centroid in the projected (lon/lat) coordinate space, not on the sphere.

Format:

`ST_Centroid (A: Geography)`

Return type: `Geography`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_AsText(ST_Centroid(ST_GeogFromWKT('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))')));
```

Output:

```
POINT (1 1)
```
