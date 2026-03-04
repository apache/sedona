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

# ST_AsHEXEWKB

Introduction: This function returns the input geometry encoded to a text representation in HEXEWKB format. The HEXEWKB encoding can use either little-endian (NDR) or big-endian (XDR) byte ordering. If no encoding is explicitly specified, the function defaults to using the little-endian (NDR) format.

Format: `ST_AsHEXEWKB(geom: Geometry, endian: String = NDR)`

Return type: `String`

SQL Example

```sql
SELECT ST_AsHEXEWKB(ST_GeomFromWKT('POINT(1 2)'), 'XDR')
```

Output:

```
00000000013FF00000000000004000000000000000
```

SQL Example

```sql
SELECT ST_AsHEXEWKB(ST_GeomFromWKT('LINESTRING (30 20, 20 25, 20 15, 30 20)'))
```

Output:

```
0102000000040000000000000000003E4000000000000034400000000000003440000000000000394000000000000034400000000000002E400000000000003E400000000000003440
```
