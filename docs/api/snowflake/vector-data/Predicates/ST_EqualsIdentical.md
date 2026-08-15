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

Unlike `ST_EqualsExact`, this predicate does not accept a tolerance. NaN ordinate values are considered equal to other NaN values. The SRID and other geometry metadata are not compared.

Format: `ST_EqualsIdentical(A: Geometry, B: Geometry)`

Return type: `Boolean`

This function supports Snowflake `GEOMETRY` values. It is not available for `GEOGRAPHY` values. Snowflake currently stores native `GEOMETRY` values as two-dimensional coordinates with 14 decimal places, so identity is evaluated on those stored 2D values.

Identical geometries return true:

```sql
SELECT SEDONA.ST_EqualsIdentical(
    ST_GeometryFromWKT('LINESTRING (0 0, 1 1)'),
    ST_GeometryFromWKT('LINESTRING (0 0, 1 1)')
)
```

Output:

```
true
```

The order of components and coordinates must match:

```sql
SELECT SEDONA.ST_EqualsIdentical(
    ST_GeometryFromWKT('LINESTRING (0 0, 1 1)'),
    ST_GeometryFromWKT('LINESTRING (1 1, 0 0)')
)
```

Output:

```
false
```

Use `ST_Equals` when only topological equality is required.
