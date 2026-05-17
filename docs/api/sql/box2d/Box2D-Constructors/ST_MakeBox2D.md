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

# ST_MakeBox2D

Introduction: Build a `Box2D` from two corner POINT geometries. The corners are taken verbatim — coordinates are not swapped or validated — so the caller is responsible for supplying a lower-left and an upper-right point. Non-POINT arguments raise an `IllegalArgumentException`.

Format: `ST_MakeBox2D(lowerLeft: Point, upperRight: Point)`

Return type: `Box2D`

Since: `v1.9.1`

SQL Example

```sql
SELECT ST_AsText(ST_MakeBox2D(ST_Point(0.0, 0.0), ST_Point(10.0, 20.0)))
```

Output:

```
BOX(0.0 0.0, 10.0 20.0)
```

Returns `NULL` if either point is `NULL` or empty.
