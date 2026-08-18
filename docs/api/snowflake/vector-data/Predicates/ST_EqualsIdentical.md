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

Since: `v2.0.0`

This function supports Snowflake `GEOMETRY` values. It is not available for `GEOGRAPHY` values. Snowflake currently stores native `GEOMETRY` values as two-dimensional coordinates with 14 decimal places, so identity is evaluated on those stored 2D values.

## Visual example

The inputs are shown separately so their traversal direction and container structure are easy to
compare. Geometries can occupy the same locations and still be non-identical when their vertex
order, geometry type, or nesting differs.

![ST_EqualsIdentical compares geometry structure and coordinate order](../../../../image/ST_EqualsIdentical/ST_EqualsIdentical_structure.svg "Structure and coordinate-order comparisons")

The Z/M visual used by the Spark and Flink pages does not apply to native Snowflake `GEOMETRY`
values because Snowflake stores those values in two dimensions.

## Choosing an equality predicate

| Predicate | Comparison | Typical use |
| --- | --- | --- |
| `ST_Equals` | Same two-dimensional topology; representation and ordering may differ | Find geometries that describe the same shape |
| `ST_EqualsExact` | Same structure and order, with corresponding stored coordinates within a tolerance | Accept small representation differences |
| `ST_EqualsIdentical` | Same type, structure, order, and exact stored coordinate values | Verify that a stored 2D geometry representation was preserved |

`ST_EqualsIdentical` does not compare SRID or other geometry metadata. The Snowflake GeoJSON UDF
bridge also does not carry SRID into the predicate, so compare CRS metadata separately when it is
part of the validation contract.

## SQL examples

### Identical geometries

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

### Topological equality versus identity

These paths occupy the same locations, so `SEDONA.ST_Equals` returns true. Their coordinate
sequences run in opposite directions, so `SEDONA.ST_EqualsIdentical` returns false.

```sql
WITH paths AS (
    SELECT
        ST_GeometryFromWKT('LINESTRING (0 0, 1 1, 2 0)') AS forward_path,
        ST_GeometryFromWKT('LINESTRING (2 0, 1 1, 0 0)') AS reverse_path
)
SELECT
    SEDONA.ST_Equals(forward_path, reverse_path) AS same_shape,
    SEDONA.ST_EqualsIdentical(forward_path, reverse_path) AS identical
FROM paths;
```

Output:

```text
same_shape | identical
-----------+----------
true       | false
```

### Geometry type and nesting

The same visible points are not identical when their geometry types and nesting differ.

```sql
SELECT SEDONA.ST_EqualsIdentical(
    ST_GeometryFromWKT('MULTIPOINT ((0 0), (4 0))'),
    ST_GeometryFromWKT(
        'GEOMETRYCOLLECTION (POINT (0 0), POINT (4 0))'
    )
) AS identical;
```

Output:

```text
identical
---------
false
```

### NULL input

The generated UDF uses Snowflake's `RETURNS NULL ON NULL INPUT` behavior, so a NULL operand
produces NULL.

```sql
SELECT SEDONA.ST_EqualsIdentical(
    NULL,
    ST_GeometryFromWKT('POINT (1 2)')
) AS identical;
```

Output:

```text
identical
---------
NULL
```

## Practical use: geometry round-trip validation

Use `ST_EqualsIdentical` for deterministic change detection after serialization, storage, or an
ETL rewrite. It can reveal a reversed coordinate sequence or changed geometry container even
when `ST_Equals` reports the same shape.

The following query flags rows whose geometry representation changed:

```sql
SELECT
    record_id,
    CASE
        WHEN source_geometry IS NULL OR reloaded_geometry IS NULL THEN 'missing_geometry'
        WHEN SEDONA.ST_EqualsIdentical(source_geometry, reloaded_geometry) THEN 'unchanged'
        ELSE 'changed'
    END AS validation_result
FROM geometry_round_trip;
```

This predicate checks geometric content, not a byte-for-byte encoding. Compare CRS and application
metadata separately when those fields are part of the validation contract. Where the input
geometry types support topological equality, use `SEDONA.ST_Equals` in a separate comparison to
distinguish representation-only changes from changed topology.
