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

# ST_EqualsIdentical

Introduction: Returns true if A and B have identical geometry types, component ordering, coordinate ordering, dimensionality, and coordinate values.

Unlike `ST_EqualsExact`, this predicate compares every available coordinate dimension, including Z and M, and does not accept a tolerance. NaN ordinate values are considered equal to other NaN values. The SRID and other geometry metadata are not compared.

Format: `ST_EqualsIdentical(A: Geometry, B: Geometry)`

Return type: `Boolean`

Since: `v2.0.0`

Identical geometries return true:

```sql
SELECT ST_EqualsIdentical(
    ST_GeomFromWKT('POINT Z (1 2 3)'),
    ST_GeomFromWKT('POINT Z (1 2 3)')
)
```

Output:

```
true
```

All dimensions must match:

```sql
SELECT ST_EqualsIdentical(
    ST_GeomFromWKT('POINT Z (1 2 3)'),
    ST_GeomFromWKT('POINT Z (1 2 4)')
)
```

Output:

```
false
```

The order of components and coordinates must also match. Use `ST_Equals` when only topological equality is required.
