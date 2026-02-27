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

# ST_Force3DM

Introduction: Forces the geometry into XYM mode. Retains any existing M coordinate, but removes the Z coordinate if present. Assigns a default M value of 0.0 if `mValue` is not specified.

!!!Note
Example output is after calling ST_AsText() on returned geometry, which adds M for in the WKT.

Format: `ST_Force3DM(geometry: Geometry, mValue: Double = 0.0)`

Since: `v1.6.1`

SQL Example

```sql
SELECT ST_AsText(ST_Force3DM(ST_GeomFromText('POLYGON M((0 0 3,0 5 3,5 0 3,0 0 3),(1 1 3,3 1 3,1 3 3,1 1 3))'), 2.3))
```

Output:

```
POLYGON M((0 0 3, 0 5 3, 5 0 3, 0 0 3), (1 1 3, 3 1 3, 1 3 3, 1 1 3))
```

SQL Example

```sql
SELECT ST_AsText(ST_Force3DM(ST_GeomFromText('LINESTRING(0 1,1 0,2 0)'), 2.3))
```

Output:

```
LINESTRING M(0 1 2.3, 1 0 2.3, 2 0 2.3)
```

SQL Example

```sql
SELECT ST_AsText(ST_Force3DM(ST_GeomFromText('LINESTRING Z(0 1 3,1 0 3,2 0 3)'), 5))
```

Output:

```
LINESTRING M(0 1 5, 1 0 5, 2 0 5)
```
