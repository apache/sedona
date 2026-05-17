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

# Box2D Functions

The `Box2D` type in Sedona represents a planar axis-aligned bounding box — a rectangle described by four `Double` values: `xmin`, `ymin`, `xmax`, `ymax`. It is a first-class SQL type backed by a Spark UDT and serialises to a struct of four non-nullable doubles, so columns of `Box2D` round-trip natively through Parquet and align with GeoParquet 1.1 bbox covering columns.

`Box2D` complements the [Geometry](../Geometry-Functions.md) and [Geography](../geography/Geography-Functions.md) types. Use it when you need a compact, comparable bounding rectangle — for example, as a covering column on a GeoParquet table that lets the reader prune row groups, or as the join key in a spatial join that only needs an envelope-level match.

## Semantic notes

- `Box2D` values use closed-interval semantics: edge-touching boxes are considered intersecting and (per [ST_BoxContains](Box2D-Predicates/ST_BoxContains.md)) contained.
- Absence is represented by SQL `NULL` rather than an in-band sentinel.
- Bounds are required to be ordered (`xmin <= xmax`, `ymin <= ymax`). Inverted-bound values are reserved for a future antimeridian-wraparound semantics on geography bboxes; predicates and join planning throw `IllegalArgumentException` on inverted input today.
- Unlike [ST_Envelope](../Bounding-Box-Functions/ST_Envelope.md), which returns a `Geometry` polygon, [ST_Box2D](Box2D-Constructors/ST_Box2D.md) returns a typed `Box2D` value. Prefer the typed form when downstream code only needs the four bounds, and prefer the polygon when downstream code expects a `Geometry`.

## Box2D Constructors

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [ST_Box2D](Box2D-Constructors/ST_Box2D.md) | Box2D | Return the planar bounding box of a Geometry as a Box2D. | v1.9.1 |
| [ST_MakeBox2D](Box2D-Constructors/ST_MakeBox2D.md) | Box2D | Build a Box2D from two corner POINT geometries. | v1.9.1 |
| [ST_GeomFromBox2D](Box2D-Constructors/ST_GeomFromBox2D.md) | Geometry | Convert a Box2D to a closed rectangular polygon Geometry (degenerate boxes return a Point or LineString). | v1.9.1 |

## Box2D Accessors

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [ST_XMin](Box2D-Accessors/ST_XMin.md) | Double | Return the minimum X coordinate of a Box2D. | v1.9.1 |
| [ST_YMin](Box2D-Accessors/ST_YMin.md) | Double | Return the minimum Y coordinate of a Box2D. | v1.9.1 |
| [ST_XMax](Box2D-Accessors/ST_XMax.md) | Double | Return the maximum X coordinate of a Box2D. | v1.9.1 |
| [ST_YMax](Box2D-Accessors/ST_YMax.md) | Double | Return the maximum Y coordinate of a Box2D. | v1.9.1 |

The same `ST_XMin` / `ST_YMin` / `ST_XMax` / `ST_YMax` functions also accept `Geometry` inputs — see [Bounding Box Functions](../Geometry-Functions.md#bounding-box-functions).

## Box2D Predicates

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [ST_BoxIntersects](Box2D-Predicates/ST_BoxIntersects.md) | Boolean | Closed-interval bbox intersection over two Box2D arguments. Matches PostGIS `&&` on `box2d`. | v1.9.1 |
| [ST_BoxContains](Box2D-Predicates/ST_BoxContains.md) | Boolean | Closed-interval bbox containment over two Box2D arguments. Matches PostGIS `~` on `box2d`. | v1.9.1 |

## Box2D Functions

| Function | Return type | Description | Since |
| :--- | :--- | :--- | :--- |
| [ST_Expand](Box2D-Functions/ST_Expand.md) | Box2D | Expand a Box2D by a per-axis or uniform delta. | v1.9.1 |
| [ST_AsText](Box2D-Functions/ST_AsText.md) | String | Return the `BOX(xmin ymin, xmax ymax)` text representation of a Box2D. | v1.9.1 |

## Type conversion

Catalyst recognises SQL `CAST` between `Box2D` and `Geometry`:

| Cast                       | Equivalent function                                                | Notes |
| :---                       | :---                                                               | :--- |
| `CAST(geom AS box2d)`      | [ST_Box2D(geom)](Box2D-Constructors/ST_Box2D.md)                   | Planar bounding box of the geometry. |
| `CAST(box AS geometry)`    | [ST_GeomFromBox2D(box)](Box2D-Constructors/ST_GeomFromBox2D.md)    | Closed rectangular polygon (Point/LineString for degenerate boxes). |

The cast forms require the Sedona SQL parser extension (`spark.sql.extensions=org.apache.sedona.sql.SedonaSqlExtensions`); the function forms work in any Sedona-enabled session.

## Query optimization

Box2D-typed columns are first-class participants in Sedona's spatial optimizer:

- **Filter pushdown.** `ST_BoxIntersects` / `ST_BoxContains` predicates over a Box2D column and a literal Box2D push down to Parquet row-group statistics on the column's underlying `xmin` / `ymin` / `xmax` / `ymax` leaves. See [Query optimization → Box2D filter pushdown](../Optimizer.md#box2d-filter-pushdown).
- **Spatial joins.** `ST_BoxIntersects` and `ST_BoxContains` route through the same physical operators (`RangeJoinExec`, `BroadcastIndexJoinExec`) used for `ST_Intersects` / `ST_Covers`. See [Query optimization → Range join](../Optimizer.md#range-join) and [Broadcast index join](../Optimizer.md#broadcast-index-join).
