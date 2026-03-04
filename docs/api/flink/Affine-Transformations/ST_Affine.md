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

# ST_Affine

Introduction: Apply an affine transformation to the given geometry.

ST_Affine has 2 overloaded signatures:

`ST_Affine(geometry, a, b, c, d, e, f, g, h, i, xOff, yOff, zOff)`

`ST_Affine(geometry, a, b, d, e, xOff, yOff)`

Based on the invoked function, the following transformation is applied:

`x = a * x + b * y + c * z + xOff OR x = a * x + b * y + xOff`

`y = d * x + e * y + f * z + yOff OR y = d * x + e * y + yOff`

`z = g * x + f * y + i * z + zOff OR z = g * x + f * y + zOff`

If the given geometry is empty, the result is also empty.

Format:

`ST_Affine(geometry, a, b, c, d, e, f, g, h, i, xOff, yOff, zOff)`

`ST_Affine(geometry, a, b, d, e, xOff, yOff)`

Return type: `Geometry`

Since: `v1.5.0`

Examples:

```sql
ST_Affine(geometry, 1, 2, 4, 1, 1, 2, 3, 2, 5, 4, 8, 3)
```

Input: `LINESTRING EMPTY`

Output: `LINESTRING EMPTY`

Input: `POLYGON ((1 0 1, 1 1 1, 2 2 2, 1 0 1))`

Output: `POLYGON Z((9 11 11, 11 12 13, 18 16 23, 9 11 11))`

Input: `POLYGON ((1 0, 1 1, 2 1, 2 0, 1 0), (1 0.5, 1 0.75, 1.5 0.75, 1.5 0.5, 1 0.5))`

Output: `POLYGON((5 9, 7 10, 8 11, 6 10, 5 9), (6 9.5, 6.5 9.75, 7 10.25, 6.5 10, 6 9.5))`

```sql
ST_Affine(geometry, 1, 2, 1, 2, 1, 2)
```

Input: `POLYGON EMPTY`

Output: `POLYGON EMPTY`

Input: `GEOMETRYCOLLECTION (MULTIPOLYGON (((1 0, 1 1, 2 1, 2 0, 1 0), (1 0.5, 1 0.75, 1.5 0.75, 1.5 0.5, 1 0.5)), ((5 0, 5 5, 7 5, 7 0, 5 0))), POINT (10 10))`

Output: `GEOMETRYCOLLECTION (MULTIPOLYGON (((2 3, 4 5, 5 6, 3 4, 2 3), (3 4, 3.5 4.5, 4 5, 3.5 4.5, 3 4)), ((6 7, 16 17, 18 19, 8 9, 6 7))), POINT (31 32))`

Input: `POLYGON ((1 0 1, 1 1 1, 2 2 2, 1 0 1))`

Output: `POLYGON Z((2 3 1, 4 5 1, 7 8 2, 2 3 1))`
