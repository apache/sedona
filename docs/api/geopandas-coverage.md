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

# GeoPandas API coverage

The `sedona.spark.geopandas` package provides a GeoPandas-style API on Apache Spark.
This reference describes **Sedona 2.0.0 (development)** against the **GeoPandas 1.1.4** API catalog.
These counts describe the development branch and do not apply to Sedona 1.9.1.
For setup and examples, see the [GeoPandas programming guide](../tutorial/geopandas-api.md).
Import `sedona.spark.geopandas` instead of `geopandas`; the tables use upstream
API names so they can be compared directly with the GeoPandas catalog.

## Coverage summary

**91.4% API availability (160 of 175 catalogued APIs)**, including partial implementations.

Availability is `(Available + Partial) / Total`. It measures whether a named API
has an implementation, not complete parameter support, identical results, or
fully distributed execution. The notes below identify known restrictions.

| API group | Available | Partial | Not implemented | Unsupported | Total | Availability |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GeoSeries | 86 | 33 | 3 | 1 | 123 | 96.7% |
| GeoDataFrame | 10 | 16 | 4 | 3 | 33 | 78.8% |
| Input/output | 0 | 3 | 2 | 0 | 5 | 60.0% |
| Tools | 1 | 5 | 2 | 0 | 8 | 75.0% |
| Spatial index | 0 | 6 | 0 | 0 | 6 | 100.0% |
| Total | 97 | 63 | 11 | 4 | 175 | 91.4% |

### Status definitions

- **Available**: implemented, with no additional API-specific restriction identified in this catalog. The common differences below still apply.
- **Partial**: implemented with parameter, result, or execution limitations described in the notes.
- **Not implemented**: absent or raises `NotImplementedError` without providing the operation.
- **Unsupported**: intentionally excluded from the distributed API.

### What is counted

The denominator contains the unique named methods, properties, and module functions in the
GeoPandas 1.1.4 reference catalogs for
[GeoSeries](https://github.com/geopandas/geopandas/blob/v1.1.4/doc/source/docs/reference/geoseries.rst),
[GeoDataFrame](https://github.com/geopandas/geopandas/blob/v1.1.4/doc/source/docs/reference/geodataframe.rst),
[input/output](https://github.com/geopandas/geopandas/blob/v1.1.4/doc/source/docs/reference/io.rst),
[tools](https://github.com/geopandas/geopandas/blob/v1.1.4/doc/source/docs/reference/tools.rst), and
[spatial indexing](https://github.com/geopandas/geopandas/blob/v1.1.4/doc/source/docs/reference/sindex.rst).

Each qualified name is counted once, including missing and intentionally unsupported APIs.
For example, `GeoDataFrame.clip` and `geopandas.clip` are separate catalog entries;
repeated listings of `GeoSeries.boundary` count only once. The `GeoSeries` and
`GeoDataFrame` class constructor entries, testing utilities, and the general pandas API
inherited through pandas-on-Spark are excluded. Alternate constructors such as
`GeoDataFrame.from_features` are included.
GeoSeries operations inherited by GeoDataFrame are not counted again unless the
GeoDataFrame catalog lists them explicitly. API links below open the upstream
GeoPandas reference; they describe the target interface, not Sedona's support level.

## Common differences

- **Execution and ordering:** operations use Spark and generally evaluate lazily. Row order is not guaranteed unless an API explicitly preserves it. Follow pandas-on-Spark guidance for index alignment and operations across different frames.
- **Geometry engines:** Sedona generally uses JTS, whereas GeoPandas uses Shapely/GEOS. Geometry ordering and some algorithm results can differ. Spatial measurements are planar; use an appropriate projected CRS for distances and areas.
- **Local results:** plotting and some conversion methods collect distributed data to the driver. These are marked Partial. Scalar aggregations and constructors that start with local data are not marked Partial solely because their result or input is local.
- **Nearest joins:** `sjoin_nearest` supports point inputs and inner joins. Ties follow Sedona configuration and do not guarantee GeoPandas' complete set of tied rows. See [nearest point joins](../tutorial/geopandas-api.md#nearest-point-joins).
- **Coordinate transforms:** `GeoSeries.transform` runs a callable on worker batches, so a callable that depends on the whole coordinate array can produce different results. It requires Spark 3.5+ and Shapely 2+.

## GeoSeries

| API | Status | Notes |
| --- | --- | --- |
| [`GeoSeries.area`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.area.html) | Available | — |
| [`GeoSeries.boundary`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.boundary.html) | Available | — |
| [`GeoSeries.bounds`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.bounds.html) | Available | — |
| [`GeoSeries.total_bounds`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.total_bounds.html) | Available | Distributed aggregation returns four bounds to the driver. |
| [`GeoSeries.length`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.length.html) | Available | — |
| [`GeoSeries.geom_type`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.geom_type.html) | Partial | Standalone LinearRing geometries are represented as LineString. |
| [`GeoSeries.offset_curve`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.offset_curve.html) | Partial | Scalar distance only; join_style and mitre_limit are accepted but ignored. |
| [`GeoSeries.distance`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.distance.html) | Available | — |
| [`GeoSeries.hausdorff_distance`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.hausdorff_distance.html) | Available | — |
| [`GeoSeries.frechet_distance`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.frechet_distance.html) | Partial | densify is not supported. |
| [`GeoSeries.representative_point`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.representative_point.html) | Available | — |
| [`GeoSeries.exterior`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.exterior.html) | Partial | Returns LineString rings instead of LinearRing geometries. |
| [`GeoSeries.interiors`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.interiors.html) | Partial | Interior rings are returned as LineString geometries instead of LinearRing objects. |
| [`GeoSeries.minimum_bounding_radius`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.minimum_bounding_radius.html) | Available | — |
| [`GeoSeries.minimum_clearance`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.minimum_clearance.html) | Available | — |
| [`GeoSeries.x`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.x.html) | Available | — |
| [`GeoSeries.y`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.y.html) | Available | — |
| [`GeoSeries.z`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.z.html) | Available | — |
| [`GeoSeries.m`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.m.html) | Available | — |
| [`GeoSeries.get_coordinates`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.get_coordinates.html) | Available | — |
| [`GeoSeries.count_coordinates`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.count_coordinates.html) | Available | — |
| [`GeoSeries.count_geometries`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.count_geometries.html) | Available | — |
| [`GeoSeries.count_interior_rings`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.count_interior_rings.html) | Available | — |
| [`GeoSeries.set_precision`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.set_precision.html) | Not implemented | Explicit NotImplementedError stub. |
| [`GeoSeries.get_precision`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.get_precision.html) | Not implemented | Explicit NotImplementedError stub. |
| [`GeoSeries.get_geometry`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.get_geometry.html) | Available | — |
| [`GeoSeries.is_closed`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.is_closed.html) | Partial | Standalone LinearRing identity is lost; empty LinearRing behavior differs. |
| [`GeoSeries.is_empty`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.is_empty.html) | Available | — |
| [`GeoSeries.is_ring`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.is_ring.html) | Available | — |
| [`GeoSeries.is_simple`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.is_simple.html) | Available | — |
| [`GeoSeries.is_valid`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.is_valid.html) | Available | — |
| [`GeoSeries.is_valid_reason`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.is_valid_reason.html) | Available | — |
| [`GeoSeries.is_valid_coverage`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.is_valid_coverage.html) | Available | Distributed validation returns a single boolean to the driver. |
| [`GeoSeries.invalid_coverage_edges`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.invalid_coverage_edges.html) | Available | — |
| [`GeoSeries.has_m`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.has_m.html) | Available | — |
| [`GeoSeries.has_z`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.has_z.html) | Available | — |
| [`GeoSeries.is_ccw`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.is_ccw.html) | Available | — |
| [`GeoSeries.contains`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.contains.html) | Available | — |
| [`GeoSeries.contains_properly`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.contains_properly.html) | Available | — |
| [`GeoSeries.crosses`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.crosses.html) | Available | — |
| [`GeoSeries.disjoint`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.disjoint.html) | Available | — |
| [`GeoSeries.dwithin`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.dwithin.html) | Available | — |
| [`GeoSeries.geom_equals`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.geom_equals.html) | Available | — |
| [`GeoSeries.geom_equals_exact`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.geom_equals_exact.html) | Partial | Standalone LinearRing and matching LineString inputs cannot be distinguished. |
| [`GeoSeries.geom_equals_identical`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.geom_equals_identical.html) | Partial | Comparison uses Spark storage: LinearRing identity and some empty/NaN-Z or mixed coordinate layouts cannot be distinguished. |
| [`GeoSeries.intersects`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.intersects.html) | Available | — |
| [`GeoSeries.overlaps`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.overlaps.html) | Available | — |
| [`GeoSeries.touches`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.touches.html) | Available | — |
| [`GeoSeries.within`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.within.html) | Available | — |
| [`GeoSeries.covers`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.covers.html) | Available | — |
| [`GeoSeries.covered_by`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.covered_by.html) | Available | — |
| [`GeoSeries.relate`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.relate.html) | Available | — |
| [`GeoSeries.relate_pattern`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.relate_pattern.html) | Available | — |
| [`GeoSeries.clip_by_rect`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.clip_by_rect.html) | Partial | Uses intersection with an envelope; boundary-only intersections and empty output types differ from GeoPandas rectangle clipping. |
| [`GeoSeries.difference`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.difference.html) | Partial | GeometryCollection operands are not generally supported by the underlying difference operation. |
| [`GeoSeries.intersection`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.intersection.html) | Available | — |
| [`GeoSeries.symmetric_difference`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.symmetric_difference.html) | Available | — |
| [`GeoSeries.union`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.union.html) | Available | — |
| [`GeoSeries.buffer`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.buffer.html) | Partial | Array/Series distances are not supported; extra keyword arguments are ignored. |
| [`GeoSeries.centroid`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.centroid.html) | Available | — |
| [`GeoSeries.concave_hull`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.concave_hull.html) | Available | — |
| [`GeoSeries.convex_hull`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.convex_hull.html) | Available | — |
| [`GeoSeries.envelope`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.envelope.html) | Available | — |
| [`GeoSeries.extract_unique_points`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.extract_unique_points.html) | Available | — |
| [`GeoSeries.force_2d`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.force_2d.html) | Available | — |
| [`GeoSeries.force_3d`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.force_3d.html) | Available | — |
| [`GeoSeries.make_valid`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.make_valid.html) | Partial | Only method='structure' is supported; the default method='linework' raises. |
| [`GeoSeries.minimum_bounding_circle`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.minimum_bounding_circle.html) | Available | — |
| [`GeoSeries.maximum_inscribed_circle`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.maximum_inscribed_circle.html) | Available | — |
| [`GeoSeries.minimum_clearance_line`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.minimum_clearance_line.html) | Available | — |
| [`GeoSeries.minimum_rotated_rectangle`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.minimum_rotated_rectangle.html) | Available | — |
| [`GeoSeries.normalize`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.normalize.html) | Available | — |
| [`GeoSeries.orient_polygons`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.orient_polygons.html) | Available | — |
| [`GeoSeries.remove_repeated_points`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.remove_repeated_points.html) | Available | — |
| [`GeoSeries.reverse`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.reverse.html) | Available | — |
| [`GeoSeries.sample_points`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.sample_points.html) | Partial | Only method='uniform'; RNG results/state advancement differ; extra keywords are ignored. |
| [`GeoSeries.segmentize`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.segmentize.html) | Available | — |
| [`GeoSeries.shortest_line`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.shortest_line.html) | Available | — |
| [`GeoSeries.simplify`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.simplify.html) | Available | — |
| [`GeoSeries.simplify_coverage`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.simplify_coverage.html) | Partial | Spark Classic with checkpoint storage required; finite valid 2D polygon coverage only, at most 100,000 coordinates per geometry. Conservative simplification may retain extra vertices and never removes rings or parts. |
| [`GeoSeries.snap`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.snap.html) | Partial | Array-like tolerance is not supported. |
| [`GeoSeries.transform`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.transform.html) | Partial | Requires Spark >=3.5 and Shapely >=2.0. Callbacks run per Spark batch and must not depend on batch boundaries or mutable state. |
| [`GeoSeries.affine_transform`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.affine_transform.html) | Available | — |
| [`GeoSeries.rotate`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.rotate.html) | Available | — |
| [`GeoSeries.scale`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.scale.html) | Available | — |
| [`GeoSeries.skew`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.skew.html) | Available | — |
| [`GeoSeries.translate`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.translate.html) | Available | — |
| [`GeoSeries.interpolate`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.interpolate.html) | Available | — |
| [`GeoSeries.line_merge`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.line_merge.html) | Partial | directed=True is not supported. |
| [`GeoSeries.project`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.project.html) | Available | — |
| [`GeoSeries.shared_paths`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.shared_paths.html) | Available | — |
| [`GeoSeries.build_area`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.build_area.html) | Available | — |
| [`GeoSeries.constrained_delaunay_triangles`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.constrained_delaunay_triangles.html) | Available | — |
| [`GeoSeries.delaunay_triangles`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.delaunay_triangles.html) | Partial | Computes a separate triangulation per input geometry rather than aggregating vertices across the series. |
| [`GeoSeries.explode`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.explode.html) | Available | — |
| [`GeoSeries.intersection_all`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.intersection_all.html) | Available | Distributed aggregation returns one geometry to the driver. |
| [`GeoSeries.polygonize`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.polygonize.html) | Partial | full=True is not supported. |
| [`GeoSeries.union_all`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.union_all.html) | Partial | grid_size is not supported; non-default method values are ignored with a warning. |
| [`GeoSeries.voronoi_polygons`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.voronoi_polygons.html) | Partial | Computes a separate diagram per geometry; only_edges=True is not supported. |
| [`GeoSeries.from_arrow`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.from_arrow.html) | Available | Converts a local Arrow array through GeoPandas before constructing the distributed series. |
| [`GeoSeries.from_file`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.from_file.html) | Partial | Supports Shapefile, GeoJSON, GeoPackage and GeoParquet; uses format/table_name options and does not implement general GeoPandas reader keywords. |
| [`GeoSeries.from_wkb`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.from_wkb.html) | Partial | Only on_invalid='raise' is implemented. |
| [`GeoSeries.from_wkt`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.from_wkt.html) | Partial | Only on_invalid='raise' is implemented. |
| [`GeoSeries.from_xy`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.from_xy.html) | Partial | Additional constructor keyword arguments support name only. |
| [`GeoSeries.to_arrow`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.to_arrow.html) | Partial | Collects the entire series to the driver and delegates to local GeoPandas. |
| [`GeoSeries.to_file`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.to_file.html) | Partial | Writes GeoJSON or GeoParquet through Spark; schema, engine and GeoPandas metadata options are not implemented. |
| [`GeoSeries.to_json`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.to_json.html) | Partial | Collects the entire series through a GeoDataFrame and serializes with local GeoPandas. |
| [`GeoSeries.to_wkb`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.to_wkb.html) | Partial | Supports hex; additional Shapely serialization keyword arguments are ignored. |
| [`GeoSeries.to_wkt`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.to_wkt.html) | Partial | Additional Shapely formatting keyword arguments are ignored. |
| [`GeoSeries.crs`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.crs.html) | Available | — |
| [`GeoSeries.set_crs`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.set_crs.html) | Available | — |
| [`GeoSeries.to_crs`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.to_crs.html) | Available | — |
| [`GeoSeries.estimate_utm_crs`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.estimate_utm_crs.html) | Available | Uses aggregated bounds to select a local pyproj CRS. |
| [`GeoSeries.fillna`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.fillna.html) | Available | Distributed alignment is lazy; invalid duplicate replacement indexes raise when evaluated. limit requires global ordering. |
| [`GeoSeries.isna`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.isna.html) | Available | — |
| [`GeoSeries.notna`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.notna.html) | Available | — |
| [`GeoSeries.clip`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.clip.html) | Partial | Rectangular masks use ST_Intersection; boundary-only results can differ from GeoPandas fast rectangle clipping. |
| [`GeoSeries.plot`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.plot.html) | Partial | Collects the entire series to the driver and delegates to local GeoPandas plotting. |
| [`GeoSeries.explore`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.explore.html) | Not implemented | No GeoSeries implementation. |
| [`GeoSeries.sindex`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.sindex.html) | Partial | Returns and caches a Sedona SpatialIndex; query options and results differ from GeoPandas (see Spatial index below). |
| [`GeoSeries.has_sindex`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.has_sindex.html) | Available | — |
| [`GeoSeries.cx`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.cx.html) | Available | — |
| [`GeoSeries.__geo_interface__`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoSeries.__geo_interface__.html) | Unsupported | Intentionally excluded by the distributed API plan (issue #2230). |

## GeoDataFrame

| API | Status | Notes |
| --- | --- | --- |
| [`GeoDataFrame.from_file`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.from_file.html) | Partial | Reads Shapefile, GeoJSON, GeoPackage, and GeoParquet; GeoPandas reader options are unsupported and directory formats must be explicit. |
| [`GeoDataFrame.from_features`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.from_features.html) | Available | Builds small local feature collections through GeoPandas before distributing them; properties follow Spark schema inference. |
| [`GeoDataFrame.from_postgis`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.from_postgis.html) | Not implemented | Stub raises NotImplementedError. |
| [`GeoDataFrame.from_arrow`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.from_arrow.html) | Available | Converts local GeoArrow input through GeoPandas; to_pandas_kwargs requires GeoPandas 1.1 or newer. |
| [`GeoDataFrame.to_file`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_file.html) | Partial | Writes GeoJSON and GeoParquet through Spark; GeoPackage output, schema, engine, and GeoPandas metadata options are unsupported. |
| [`GeoDataFrame.to_json`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_json.html) | Partial | Collects the distributed frame to the driver and delegates to GeoPandas. |
| [`GeoDataFrame.to_geo_dict`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_geo_dict.html) | Unsupported | Intentionally unsupported local feature-collection interface for distributed data; current stub raises NotImplementedError. |
| [`GeoDataFrame.to_parquet`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_parquet.html) | Partial | Uses the Sedona GeoParquet writer and Spark options; GeoPandas/PyArrow writer-option parity is not provided. |
| [`GeoDataFrame.to_arrow`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_arrow.html) | Partial | Collects the distributed frame to the driver and delegates to GeoPandas; index=None includes the distributed index. |
| [`GeoDataFrame.to_feather`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_feather.html) | Not implemented | Stub raises NotImplementedError. |
| [`GeoDataFrame.to_postgis`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_postgis.html) | Not implemented | No GeoDataFrame implementation is present. |
| [`GeoDataFrame.to_wkb`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_wkb.html) | Partial | Distributed binary and hexadecimal output work; Shapely serialization keyword options are rejected. |
| [`GeoDataFrame.to_wkt`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_wkt.html) | Partial | Distributed output works; Shapely formatting keywords are rejected and stored precision is retained rather than default six-place rounding. |
| [`GeoDataFrame.crs`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.crs.html) | Available | Delegates CRS metadata access to the active geometry column. |
| [`GeoDataFrame.set_crs`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.set_crs.html) | Available | Assigns or removes active-column CRS metadata, including epsg, inplace, and allow_override. |
| [`GeoDataFrame.to_crs`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_crs.html) | Available | Transforms the active geometry column; supports crs, epsg, and inplace. |
| [`GeoDataFrame.estimate_utm_crs`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.estimate_utm_crs.html) | Available | Uses a distributed bounds aggregation; only four aggregate values reach the driver. |
| [`GeoDataFrame.rename_geometry`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.rename_geometry.html) | Available | Renames the active geometry column, with inplace support. |
| [`GeoDataFrame.set_geometry`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.set_geometry.html) | Partial | Existing-column drop=True is rejected; array-like and column selection otherwise work. |
| [`GeoDataFrame.active_geometry_name`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.active_geometry_name.html) | Available | Returns the active geometry column label, or None. |
| [`GeoDataFrame.dissolve`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.dissolve.html) | Partial | Unary union only, without grid_size; supported aggregate aliases and grouping forms are restricted, and unobserved categorical groups and MultiIndex columns are unsupported. |
| [`GeoDataFrame.explode`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.explode.html) | Available | Distributed geometry-part expansion supports column, ignore_index, and index_parts; non-geometry columns delegate to pandas-on-Spark. |
| [`GeoDataFrame.sjoin`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.sjoin.html) | Partial | Contains-properly is unavailable; dwithin requires scalar distance; right joins retain left geometries and row order is not preserved. |
| [`GeoDataFrame.sjoin_nearest`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.sjoin_nearest.html) | Partial | Point-only inner joins, default suffixes, exclusive=False, flat string columns, and single-level indexes; KNN tie behavior can omit equidistant or coincident rows. |
| [`GeoDataFrame.clip`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.clip.html) | Partial | Rectangular masks use ST_Intersection; boundary-only results can differ from GeoPandas fast rectangle clipping. |
| [`GeoDataFrame.overlay`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.overlay.html) | Partial | All five modes are distributed; MultiIndex columns are unsupported, and keep_geom_type=None omits the conditional dropped-geometry warning. |
| [`GeoDataFrame.explore`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.explore.html) | Not implemented | No GeoDataFrame implementation is present. |
| [`GeoDataFrame.plot`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.plot.html) | Partial | Collects the distributed frame to the driver and delegates to GeoPandas. |
| [`GeoDataFrame.sindex`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.sindex.html) | Partial | Returns a Sedona spatial index with different query results and options; the GeoDataFrame does not retain the index cache. |
| [`GeoDataFrame.has_sindex`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.has_sindex.html) | Partial | GeoDataFrame spatial-index state is not retained; use a retained active GeoSeries for cache state. |
| [`GeoDataFrame.cx`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.cx.html) | Available | Distributed coordinate bounding-box selection supports open, reversed, and inclusive slice bounds. |
| [`GeoDataFrame.__geo_interface__`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.__geo_interface__.html) | Unsupported | Intentionally unsupported local feature-collection interface for distributed data; current stub raises NotImplementedError. |
| [`GeoDataFrame.iterfeatures`](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.iterfeatures.html) | Unsupported | Intentionally unsupported row iterator for distributed data; current stub raises NotImplementedError. |

## Input/output

| API | Status | Notes |
| --- | --- | --- |
| [`geopandas.list_layers`](https://geopandas.org/en/stable/docs/reference/api/geopandas.list_layers.html) | Partial | GeoPackage only; string/path-like inputs only. Reads layer metadata through Spark and collects the small layer listing to pandas. |
| [`geopandas.read_file`](https://geopandas.org/en/stable/docs/reference/api/geopandas.read_file.html) | Partial | Reads Shapefile, GeoJSON, GeoPackage and GeoParquet only. GeoPandas bbox/mask/columns/rows/engine options are warned about and ignored; GeoPackage requires table_name rather than layer. |
| [`geopandas.read_postgis`](https://geopandas.org/en/stable/docs/reference/api/geopandas.read_postgis.html) | Not implemented | No implementation or public export in sedona.spark.geopandas. |
| [`geopandas.read_feather`](https://geopandas.org/en/stable/docs/reference/api/geopandas.read_feather.html) | Not implemented | No implementation or public export in sedona.spark.geopandas. |
| [`geopandas.read_parquet`](https://geopandas.org/en/stable/docs/reference/api/geopandas.read_parquet.html) | Partial | Reads GeoParquet through Spark. Named columns, storage_options, bbox and to_pandas_kwargs parameters are explicitly unsupported and silently ignored; extra kwargs warn and are ignored. |

## Tools

| API | Status | Notes |
| --- | --- | --- |
| [`geopandas.sjoin`](https://geopandas.org/en/stable/docs/reference/api/geopandas.sjoin.html) | Partial | Contains-properly is unavailable; dwithin requires scalar distance; right joins retain left geometries and row order is not preserved. |
| [`geopandas.sjoin_nearest`](https://geopandas.org/en/stable/docs/reference/api/geopandas.sjoin_nearest.html) | Partial | Point-only inner joins with exclusive=False, default suffixes, flat unique string columns and single-level indexes. All equidistant rows are not guaranteed; max_distance filters results after the KNN search. |
| [`geopandas.overlay`](https://geopandas.org/en/stable/docs/reference/api/geopandas.overlay.html) | Partial | All five overlay modes are implemented natively; MultiIndex columns are explicitly unsupported. keep_geom_type=None does not emit the conditional dropped-geometry warning. |
| [`geopandas.clip`](https://geopandas.org/en/stable/docs/reference/api/geopandas.clip.html) | Partial | Rectangular masks use ST_Intersection; boundary-only results can differ from GeoPandas fast rectangle clipping. |
| [`geopandas.tools.geocode`](https://geopandas.org/en/stable/docs/reference/api/geopandas.tools.geocode.html) | Not implemented | No geocode implementation or tools export. |
| [`geopandas.tools.reverse_geocode`](https://geopandas.org/en/stable/docs/reference/api/geopandas.tools.reverse_geocode.html) | Not implemented | No reverse_geocode implementation or tools export. |
| [`geopandas.tools.collect`](https://geopandas.org/en/stable/docs/reference/api/geopandas.tools.collect.html) | Available | Distributed inputs use ST_Collect_Agg and materialize only the metadata and one scalar geometry required by the API; local inputs delegate to GeoPandas. |
| [`geopandas.points_from_xy`](https://geopandas.org/en/stable/docs/reference/api/geopandas.points_from_xy.html) | Partial | Constructs 2D/3D points with CRS through native expressions, but returns a distributed GeoSeries instead of upstream GeometryArray. Distributed coordinate Series must share a frame and index. |

## Spatial index

| API | Status | Notes |
| --- | --- | --- |
| [`SpatialIndex.intersection`](https://geopandas.org/en/stable/docs/reference/api/geopandas.sindex.SpatialIndex.intersection.html) | Partial | Bounding-box wrapper; a distributed index collects matching geometry objects to the driver instead of returning positional indices. Bounds are required as four coordinates. |
| [`SpatialIndex.is_empty`](https://geopandas.org/en/stable/docs/reference/api/geopandas.sindex.SpatialIndex.is_empty.html) | Partial | Checks whether the input row count is zero; an input containing only null or empty geometries reports False, unlike the upstream index. |
| [`SpatialIndex.nearest`](https://geopandas.org/en/stable/docs/reference/api/geopandas.sindex.SpatialIndex.nearest.html) | Partial | Scalar Shapely input only with a Sedona k argument; no return_all, max_distance or exclusive arguments. Distributed results are collected geometry objects rather than the upstream index-pair array. |
| [`SpatialIndex.query`](https://geopandas.org/en/stable/docs/reference/api/geopandas.sindex.SpatialIndex.query.html) | Partial | Scalar Shapely input only; predicates limited to None/intersects/contains, without distance or output_format arguments. Distributed results are collected geometry objects; the local intersects path queries bounding boxes. |
| [`SpatialIndex.size`](https://geopandas.org/en/stable/docs/reference/api/geopandas.sindex.SpatialIndex.size.html) | Partial | Counts input rows, including null and empty geometries; upstream counts actual indexed geometries. |
| [`SpatialIndex.valid_query_predicates`](https://geopandas.org/en/stable/docs/reference/api/geopandas.sindex.SpatialIndex.valid_query_predicates.html) | Partial | Returns a fresh set containing only None, intersects and contains, reflecting the narrower query implementation. |

## Maintaining this reference

When API support changes, update its status and notes, recalculate the category and
overall counts, and update the percentage in the repository README. Keep the upstream
baseline version fixed unless the entire catalog is refreshed. Adding a stub does not
increase availability, and adding one supported parameter does not make an API fully
compatible. The [GeoPandas implementation tracker](https://github.com/apache/sedona/issues/2230)
records ongoing work; this page records the implementation available in the version above.
