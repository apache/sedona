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

# ST_CoverageInvalidEdgesForTarget

Introduction: Validate one target geometry against an array of neighboring
geometries in a polygonal coverage. The function returns the target boundary
edges that overlap or do not edge-match their neighbors. A coverage-valid
target, or a geometry without polygonal components, returns an empty
`LineString`. A null target, neighbor array, or gap width returns null; null
members inside the neighbor array are ignored.

The neighbor array must not contain the target itself, and must contain every
geometry whose polygonal components can interact with the target. Extra
geometries are safe but may reduce performance. When `gapWidth` is positive,
the array must include every geometry within that distance of the target. An
incomplete array can make an invalid target appear valid. To validate a
complete coverage, call the function for every polygon using its complete
neighbor array.

The function checks coverage relationships; it does not check whether
individual inputs are valid OGC geometries. Use `ST_IsValid` separately when
required.

Formats:

```sql
ST_CoverageInvalidEdgesForTarget(target: Geometry, adjacent: ARRAY[Geometry])
```

```sql
ST_CoverageInvalidEdgesForTarget(
    target: Geometry,
    adjacent: ARRAY[Geometry],
    gapWidth: Double
)
```

The default `gapWidth` is `0.0`. A positive value additionally reports edges
forming gaps up to that width. The value must be finite and non-negative.

Return type: `Geometry`

Since: `v2.0.0`

SQL Example

```sql
WITH coverage AS (
    SELECT
        ST_GeomFromWKT('POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))') AS target,
        ARRAY(
            ST_GeomFromWKT('POLYGON ((1 0, 1 1, 2 1, 2 0, 1 0))')
        ) AS matching,
        ARRAY(
            ST_GeomFromWKT('POLYGON ((0.5 0, 0.5 1, 1.5 1, 1.5 0, 0.5 0))')
        ) AS overlapping
)
SELECT
    ST_IsEmpty(ST_CoverageInvalidEdgesForTarget(target, matching)) AS matching_is_valid,
    ST_IsEmpty(
        ST_CoverageInvalidEdgesForTarget(target, overlapping)
    ) AS overlapping_is_valid
FROM coverage
```

Result:

```
+-----------------+--------------------+
|matching_is_valid|overlapping_is_valid|
+-----------------+--------------------+
|             true|               false|
+-----------------+--------------------+
```
