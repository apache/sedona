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

# ST_MakePointM

Introduction: Creates a point with X, Y, and M coordinate. Use [ST_MakePoint](ST_MakePoint.md) to make points with XY, XYZ, or XYZM coordinates.

Format: `ST_MakePointM(x: Double, y: Double, m: Double)`

Return type: `Geometry`

Since: `v1.6.1`

Example:

```sql
SELECT ST_MakePointM(1, 2, 3)
```

Output:

```
Point M(1 2 3)
```
