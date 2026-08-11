# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Distributed spatial overlay operations."""

from __future__ import annotations

import typing

import numpy as np
import pandas as pd
from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame
from pyspark.pandas.internal import (
    InternalFrame,
    NATURAL_ORDER_COLUMN_NAME,
    SPARK_DEFAULT_INDEX_NAME,
)
from pyspark.pandas.utils import scol_for
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from sedona.spark.geopandas._crs import copy_crs_metadata, warn_crs_mismatch
from sedona.spark.sql import st_aggregates as sta
from sedona.spark.sql import st_functions as stf
from sedona.spark.sql import st_predicates as stp

_ALLOWED_HOWS = [
    "intersection",
    "union",
    "identity",
    "symmetric_difference",
    "difference",
]
_POINT_TYPES = ("ST_Point", "ST_MultiPoint")
_LINE_TYPES = ("ST_LineString", "ST_MultiLineString")
_POLYGON_TYPES = ("ST_Polygon", "ST_MultiPolygon")
_GEOMETRY_COLLECTION = "ST_GeometryCollection"


class _OverlayFrame(typing.NamedTuple):
    internal: typing.Any
    sdf: typing.Any
    labels: list
    fields: list
    geometry_position: int
    geometry_name: str
    row_id_name: str
    order_name: str
    attribute_positions: list
    attribute_names: list

    @property
    def geometry_label(self):
        return self.labels[self.geometry_position]

    @property
    def geometry_field(self):
        return self.fields[self.geometry_position]

    @property
    def attribute_labels(self):
        return [self.labels[position] for position in self.attribute_positions]

    @property
    def attribute_fields(self):
        return [self.fields[position] for position in self.attribute_positions]


class _OverlayLayout(typing.NamedTuple):
    labels: list
    fields: list
    physical_names: list
    geometry_position: int
    geometry_label: tuple
    left_attribute_indices: list
    right_attribute_indices: list


def _family_expression(geometry):
    geometry_type = stf.ST_GeometryType(geometry)
    return (
        F.when(geometry_type.isin(*_POINT_TYPES), F.lit("point"))
        .when(geometry_type.isin(*_LINE_TYPES), F.lit("line"))
        .when(geometry_type.isin(*_POLYGON_TYPES), F.lit("polygon"))
    )


def _normalize_frame(frame, side: str) -> _OverlayFrame:
    internal = frame._internal.resolved_copy
    if len(internal.column_label_names) > 1:
        raise NotImplementedError(
            "overlay does not currently support GeoDataFrames with "
            "MultiIndex columns"
        )
    if len(set(internal.column_labels)) != len(internal.column_labels):
        raise ValueError("overlay does not support duplicate column labels")

    active_name = frame.active_geometry_name
    geometry_positions = [
        position
        for position, label in enumerate(internal.column_labels)
        if len(label) == 1 and label[0] == active_name
    ]
    if len(geometry_positions) != 1:
        raise AttributeError(
            "You are calling a geospatial method on a GeoDataFrame without "
            "an active geometry column."
        )
    geometry_position = geometry_positions[0]
    source_sdf = internal.spark_frame
    geometry_name = f"__overlay_{side}_geometry__"
    row_id_name = f"__overlay_{side}_row_id__"
    order_name = f"__overlay_{side}_order__"
    attribute_positions = [
        position
        for position in range(len(internal.column_labels))
        if position != geometry_position
    ]
    attribute_names = [
        f"__overlay_{side}_attribute_{position}__"
        for position in range(len(attribute_positions))
    ]
    attribute_by_position = dict(zip(attribute_positions, attribute_names))

    normalized_sdf = source_sdf.select(
        *[
            internal.data_spark_columns[position].alias(
                geometry_name
                if position == geometry_position
                else attribute_by_position[position]
            )
            for position in range(len(internal.column_labels))
        ],
        scol_for(source_sdf, NATURAL_ORDER_COLUMN_NAME).alias(row_id_name),
        scol_for(source_sdf, NATURAL_ORDER_COLUMN_NAME).alias(order_name),
    )
    return _OverlayFrame(
        internal=internal,
        sdf=normalized_sdf,
        labels=list(internal.column_labels),
        fields=list(internal.data_fields),
        geometry_position=geometry_position,
        geometry_name=geometry_name,
        row_id_name=row_id_name,
        order_name=order_name,
        attribute_positions=attribute_positions,
        attribute_names=attribute_names,
    )


def _frame_summary(frame: _OverlayFrame, prefix: str):
    geometry = scol_for(frame.sdf, frame.geometry_name)
    geometry_type = stf.ST_GeometryType(geometry)
    family = _family_expression(geometry)
    polygon = geometry_type.isin(*_POLYGON_TYPES)
    return frame.sdf.agg(
        F.count(F.lit(1)).alias(f"{prefix}_row_count"),
        F.countDistinct(family).alias(f"{prefix}_family_count"),
        F.min_by(geometry_type, scol_for(frame.sdf, frame.order_name)).alias(
            f"{prefix}_first_type"
        ),
        F.coalesce(
            F.sum(F.when(polygon, F.lit(1)).otherwise(F.lit(0))), F.lit(0)
        ).alias(f"{prefix}_polygon_count"),
        F.coalesce(
            F.sum(
                F.when(polygon & ~stf.ST_IsValid(geometry), F.lit(1)).otherwise(
                    F.lit(0)
                )
            ),
            F.lit(0),
        ).alias(f"{prefix}_invalid_count"),
    )


def _input_summary(left: _OverlayFrame, right: _OverlayFrame):
    """Materialize only bounded validation metadata, never geometry rows."""
    return (
        _frame_summary(left, "left").crossJoin(_frame_summary(right, "right")).first()
    )


def _extract_family(geometry, family: str):
    family_id = {"point": 1, "line": 2, "polygon": 3}[family]
    extracted = stf.ST_CollectionExtract(geometry, family_id)
    # GeoPandas dissolves the surviving collection components. Unary union
    # likewise unwraps a single component and merges adjacent components.
    return stf.ST_UnaryUnion(extracted)


def _repair_frame(frame: _OverlayFrame, invalid_count: int) -> _OverlayFrame:
    if invalid_count == 0:
        return frame
    geometry = scol_for(frame.sdf, frame.geometry_name)
    repaired = _extract_family(stf.ST_MakeValid(geometry), "polygon")
    repaired = stf.ST_SetSRID(repaired, stf.ST_SRID(geometry))
    repaired_sdf = frame.sdf.select(
        *[
            (
                F.when(stf.ST_IsValid(geometry), geometry)
                .otherwise(repaired)
                .alias(name)
                if name == frame.geometry_name
                else scol_for(frame.sdf, name)
            )
            for name in frame.sdf.columns
        ]
    )
    return frame._replace(sdf=repaired_sdf)


def _candidate_pairs(left: _OverlayFrame, right: _OverlayFrame):
    left_geometry = scol_for(left.sdf, left.geometry_name)
    right_geometry = scol_for(right.sdf, right.geometry_name)
    # Keep the spatial predicate directly in the join condition so Sedona's
    # optimizer can plan a RangeJoin or BroadcastIndexJoin.
    return left.sdf.join(
        right.sdf,
        stp.ST_Intersects(left_geometry, right_geometry),
        "inner",
    )


def _make_valid_polygon_result(geometry, srid):
    # Match GeoPandas' output repair without extracting or dissolving parts;
    # unary union would unwrap a valid single-part MultiPolygon to Polygon.
    valid = F.when(
        stf.ST_GeometryType(geometry).isin(*_POLYGON_TYPES),
        stf.ST_MakeValid(geometry),
    ).otherwise(geometry)
    return stf.ST_SetSRID(valid, srid)


def _intersection_geometry(pairs, left: _OverlayFrame, right: _OverlayFrame):
    left_geometry = scol_for(pairs, left.geometry_name)
    right_geometry = scol_for(pairs, right.geometry_name)
    intersection = stf.ST_Intersection(left_geometry, right_geometry)
    return _make_valid_polygon_result(intersection, stf.ST_SRID(left_geometry))


def _difference_rows(
    source: _OverlayFrame,
    mask: _OverlayFrame,
    pairs,
    side: str,
):
    mask_name = f"__overlay_{side}_mask__"
    grouped_masks = pairs.groupBy(source.row_id_name).agg(
        sta.ST_Union_Aggr(scol_for(pairs, mask.geometry_name)).alias(mask_name)
    )
    joined = source.sdf.join(grouped_masks, source.row_id_name, "left")
    source_geometry = scol_for(joined, source.geometry_name)
    mask_geometry = scol_for(joined, mask_name)
    difference = F.when(mask_geometry.isNull(), source_geometry).otherwise(
        stf.ST_Difference(source_geometry, mask_geometry)
    )
    difference = _make_valid_polygon_result(
        difference, stf.ST_SRID(source_geometry)
    ).alias("__overlay_difference_geometry__")
    result = joined.select(
        *[scol_for(joined, name) for name in source.attribute_names],
        difference,
    )
    geometry = scol_for(result, "__overlay_difference_geometry__")
    # GeoPandas retains null differences but removes typed empty geometries.
    return result.where(geometry.isNull() | ~stf.ST_IsEmpty(geometry))


def _suffix_label(label: tuple, suffix: str) -> tuple:
    return (f"{label[0]}{suffix}",)


def _promote_nullable_field(field):
    """Match pandas concat promotion when a branch supplies typed nulls."""
    if field.is_extension_dtype:
        return field.copy(nullable=True)
    if pd.api.types.is_integer_dtype(field.dtype):
        return field.copy(
            dtype=np.dtype("float64"),
            spark_type=DoubleType(),
            nullable=True,
        )
    if pd.api.types.is_bool_dtype(field.dtype):
        return field.copy(dtype=np.dtype("object"), nullable=True)
    return field.copy(nullable=True)


def _common_layout(
    left: _OverlayFrame, right: _OverlayFrame, how: str
) -> _OverlayLayout:
    geometry_label = ("geometry",)
    left_indices = [
        position
        for position, label in enumerate(left.attribute_labels)
        if how != "symmetric_difference" or label != ("geometry",)
    ]
    right_indices = [
        position
        for position, label in enumerate(right.attribute_labels)
        if how != "symmetric_difference" or label != ("geometry",)
    ]
    left_labels = [left.attribute_labels[position] for position in left_indices]
    right_labels = [right.attribute_labels[position] for position in right_indices]
    overlap = set(left_labels) & set(right_labels)
    promote_left = how in ("symmetric_difference", "union")
    promote_right = how in ("identity", "symmetric_difference", "union")
    left_specs = [
        (
            position,
            _suffix_label(label, "_1") if label in overlap else label,
            (
                _promote_nullable_field(left.attribute_fields[position])
                if promote_left or (how == "identity" and label == geometry_label)
                else left.attribute_fields[position]
            ),
        )
        for position, label in zip(left_indices, left_labels)
    ]
    right_specs = [
        (
            position,
            _suffix_label(label, "_2") if label in overlap else label,
            (
                _promote_nullable_field(right.attribute_fields[position])
                if promote_right
                else right.attribute_fields[position]
            ),
        )
        for position, label in zip(right_indices, right_labels)
    ]
    combined_specs = [("left", *spec) for spec in left_specs] + [
        ("right", *spec) for spec in right_specs
    ]
    reserved_positions = [
        position
        for position, (_, _, label, _) in enumerate(combined_specs)
        if label == geometry_label
    ]
    combined_specs = [spec for spec in combined_specs if spec[2] != geometry_label]
    geometry_position = (
        reserved_positions[0]
        if how == "intersection" and reserved_positions
        else len(combined_specs)
    )
    result_labels = [spec[2] for spec in combined_specs]
    result_labels.insert(geometry_position, geometry_label)
    if len(set(result_labels)) != len(result_labels):
        raise ValueError(
            "overlay suffixes produce duplicate output column labels; rename "
            "the conflicting columns before calling overlay"
        )
    fields = [spec[3] for spec in combined_specs]
    fields.insert(geometry_position, left.geometry_field)
    left_indices = [spec[1] for spec in combined_specs if spec[0] == "left"]
    right_indices = [spec[1] for spec in combined_specs if spec[0] == "right"]
    physical_names = [
        f"__overlay_output_{position}__" for position in range(len(result_labels))
    ]
    return _OverlayLayout(
        labels=result_labels,
        fields=fields,
        physical_names=physical_names,
        geometry_position=geometry_position,
        geometry_label=geometry_label,
        left_attribute_indices=left_indices,
        right_attribute_indices=right_indices,
    )


def _difference_layout(left: _OverlayFrame) -> _OverlayLayout:
    physical_names = [
        f"__overlay_output_{position}__" for position in range(len(left.labels))
    ]
    return _OverlayLayout(
        labels=list(left.labels),
        fields=list(left.fields),
        physical_names=physical_names,
        geometry_position=left.geometry_position,
        geometry_label=left.geometry_label,
        left_attribute_indices=list(range(len(left.attribute_names))),
        right_attribute_indices=[],
    )


def _intersection_result(
    pairs,
    left: _OverlayFrame,
    right: _OverlayFrame,
    layout: _OverlayLayout,
):
    geometry = _intersection_geometry(pairs, left, right)
    expressions = []
    attribute_output_names = [
        name
        for position, name in enumerate(layout.physical_names)
        if position != layout.geometry_position
    ]
    attribute_output_fields = [
        field
        for position, field in enumerate(layout.fields)
        if position != layout.geometry_position
    ]
    output_position = 0
    for position in layout.left_attribute_indices:
        name = left.attribute_names[position]
        value = scol_for(pairs, name)
        target_field = attribute_output_fields[output_position]
        if left.attribute_fields[position].spark_type != target_field.spark_type:
            value = value.cast(target_field.spark_type)
        expressions.append(value.alias(attribute_output_names[output_position]))
        output_position += 1
    for position in layout.right_attribute_indices:
        name = right.attribute_names[position]
        value = scol_for(pairs, name)
        target_field = attribute_output_fields[output_position]
        if right.attribute_fields[position].spark_type != target_field.spark_type:
            value = value.cast(target_field.spark_type)
        expressions.append(value.alias(attribute_output_names[output_position]))
        output_position += 1
    expressions.append(geometry.alias(layout.physical_names[layout.geometry_position]))
    result = pairs.select(*expressions)
    result_geometry = scol_for(result, layout.physical_names[layout.geometry_position])
    return result.where(result_geometry.isNotNull() & ~stf.ST_IsEmpty(result_geometry))


def _typed_null(field):
    return F.lit(None).cast(field.spark_type)


def _common_difference_result(
    difference,
    source: _OverlayFrame,
    layout: _OverlayLayout,
    source_is_left: bool,
):
    attribute_output_fields = [
        field
        for position, field in enumerate(layout.fields)
        if position != layout.geometry_position
    ]
    left_count = len(layout.left_attribute_indices)
    if source_is_left:
        source_indices = layout.left_attribute_indices
        source_specs = [
            (
                (
                    _typed_null(attribute_output_fields[output_position])
                    if source.attribute_labels[position] == ("geometry",)
                    else scol_for(difference, source.attribute_names[position])
                ),
                (
                    attribute_output_fields[output_position]
                    if source.attribute_labels[position] == ("geometry",)
                    else source.attribute_fields[position]
                ),
            )
            for output_position, position in enumerate(source_indices)
        ]
        value_specs = [
            *source_specs,
            *[
                (_typed_null(field), field)
                for field in attribute_output_fields[left_count:]
            ],
        ]
    else:
        source_indices = layout.right_attribute_indices
        source_specs = [
            (
                (
                    _typed_null(attribute_output_fields[left_count + output_position])
                    if source.attribute_labels[position] == ("geometry",)
                    else scol_for(difference, source.attribute_names[position])
                ),
                (
                    attribute_output_fields[left_count + output_position]
                    if source.attribute_labels[position] == ("geometry",)
                    else source.attribute_fields[position]
                ),
            )
            for output_position, position in enumerate(source_indices)
        ]
        value_specs = [
            *[
                (_typed_null(field), field)
                for field in attribute_output_fields[:left_count]
            ],
            *source_specs,
        ]
    values = [
        (
            value.cast(target_field.spark_type)
            if source_field.spark_type != target_field.spark_type
            else value
        )
        for (value, source_field), target_field in zip(
            value_specs, attribute_output_fields
        )
    ]
    geometry_name = layout.physical_names[layout.geometry_position]
    attribute_output_names = [
        name
        for position, name in enumerate(layout.physical_names)
        if position != layout.geometry_position
    ]
    return difference.select(
        *[value.alias(name) for value, name in zip(values, attribute_output_names)],
        scol_for(difference, "__overlay_difference_geometry__").alias(geometry_name),
    )


def _standalone_difference_result(
    difference,
    left: _OverlayFrame,
    layout: _OverlayLayout,
):
    attributes = dict(zip(left.attribute_positions, left.attribute_names))
    return difference.select(
        *[
            (
                scol_for(difference, "__overlay_difference_geometry__").alias(
                    layout.physical_names[position]
                )
                if position == left.geometry_position
                else scol_for(difference, attributes[position]).alias(
                    layout.physical_names[position]
                )
            )
            for position in range(len(left.labels))
        ]
    )


def _family_from_type(geometry_type):
    if geometry_type in _POINT_TYPES:
        return "point"
    if geometry_type in _LINE_TYPES:
        return "line"
    if geometry_type in _POLYGON_TYPES:
        return "polygon"
    return None


def _keep_geometry_family(sdf, layout: _OverlayLayout, family: str):
    geometry_name = layout.physical_names[layout.geometry_position]
    geometry = scol_for(sdf, geometry_name)
    geometry_type = stf.ST_GeometryType(geometry)
    allowed = {
        "point": _POINT_TYPES,
        "line": _LINE_TYPES,
        "polygon": _POLYGON_TYPES,
    }[family]
    extracted = _extract_family(geometry, family)
    replacement = F.when(geometry_type == _GEOMETRY_COLLECTION, extracted).otherwise(
        geometry
    )
    filtered = sdf.select(
        *[
            (replacement.alias(name) if name == geometry_name else scol_for(sdf, name))
            for name in sdf.columns
        ]
    )
    result_geometry = scol_for(filtered, geometry_name)
    return filtered.where(
        stf.ST_GeometryType(result_geometry).isin(*allowed)
        & ~stf.ST_IsEmpty(result_geometry)
    )


def _finalize(
    sdf,
    layout: _OverlayLayout,
    left: _OverlayFrame,
    output_srid,
):
    geometry_name = layout.physical_names[layout.geometry_position]
    sdf = sdf.select(
        *[
            (
                stf.ST_SetSRID(scol_for(sdf, name), output_srid).alias(name)
                if name == geometry_name
                else scol_for(sdf, name)
            )
            for name in sdf.columns
        ]
    )
    indexed = InternalFrame.attach_distributed_sequence_column(
        sdf, SPARK_DEFAULT_INDEX_NAME
    )
    output_sdf = indexed.select(
        scol_for(indexed, SPARK_DEFAULT_INDEX_NAME),
        *[scol_for(indexed, name) for name in layout.physical_names],
        scol_for(indexed, SPARK_DEFAULT_INDEX_NAME).alias(NATURAL_ORDER_COLUMN_NAME),
    )
    data_fields = []
    for position, (source_field, physical_name) in enumerate(
        zip(layout.fields, layout.physical_names)
    ):
        result_field = source_field.copy(
            name=physical_name,
            spark_type=output_sdf.schema[physical_name].dataType,
            nullable=output_sdf.schema[physical_name].nullable,
        )
        if position == layout.geometry_position:
            result_field = copy_crs_metadata(left.geometry_field, result_field)
        data_fields.append(result_field)

    result_internal = InternalFrame(
        spark_frame=output_sdf,
        index_spark_columns=[scol_for(output_sdf, SPARK_DEFAULT_INDEX_NAME)],
        index_names=[None],
        column_labels=layout.labels,
        data_spark_columns=[
            scol_for(output_sdf, name) for name in layout.physical_names
        ],
        data_fields=data_fields,
        column_label_names=list(left.internal.column_label_names),
    )
    from sedona.spark.geopandas.geodataframe import GeoDataFrame

    result = GeoDataFrame(PandasOnSparkDataFrame(result_internal))
    result._geometry_column_name = layout.geometry_label[0]
    return result


def overlay(
    df1,
    df2,
    how: str = "intersection",
    keep_geom_type=None,
    make_valid: bool = True,
):
    """Perform a distributed spatial overlay between two GeoDataFrames.

    Parameters
    ----------
    df1, df2 : GeoDataFrame
        Distributed frames with one active geometry column each. Each frame
        must contain only one basic geometry family.
    how : {"intersection", "union", "identity", "symmetric_difference", "difference"}
        Overlay operation to perform.
    keep_geom_type : bool, default None
        Keep only geometries in the family of ``df1``. ``None`` has the same
        filtering behavior as ``True``. Unlike local GeoPandas, the distributed
        implementation does not eagerly execute the output solely to emit a
        conditional warning when geometries are dropped.
    make_valid : bool, default True
        Repair invalid polygon inputs before overlay. If False, invalid
        polygon-only inputs raise ``ValueError``.

    Returns
    -------
    GeoDataFrame
        A distributed overlay result with a fresh index. Constructing the
        result eagerly runs one distributed validation and metadata
        aggregation over both inputs; the overlay geometry rows remain lazily
        evaluated.

    Notes
    -----
    Candidate pairs use Sedona's native spatial join. Difference modes union
    each source row's matched neighbours on the cluster before applying
    ``ST_Difference``; no geometry rows are collected to the driver. JTS and
    GEOS may choose different component or coordinate ordering for
    topologically equivalent results. Composite modes deliberately recompute
    candidate joins instead of implicitly persisting a potentially large pair
    relation. ``identity`` follows GeoPandas 1.1+ dtype semantics and preserves
    left-side attribute dtypes.
    """
    from sedona.spark.geopandas.geodataframe import GeoDataFrame
    from sedona.spark.geopandas.geoseries import GeoSeries

    if how not in _ALLOWED_HOWS:
        raise ValueError(f"`how` was '{how}' but is expected to be in {_ALLOWED_HOWS}")
    if isinstance(df1, GeoSeries) or isinstance(df2, GeoSeries):
        raise NotImplementedError(
            "overlay currently only implemented for GeoDataFrames"
        )
    if not isinstance(df1, GeoDataFrame) or not isinstance(df2, GeoDataFrame):
        raise TypeError("overlay expects two GeoDataFrame inputs")
    if keep_geom_type is not None:
        if not isinstance(keep_geom_type, (bool, np.bool_)):
            raise TypeError("'keep_geom_type' must be a boolean or None")
        keep_geom_type = bool(keep_geom_type)
    if not isinstance(make_valid, (bool, np.bool_)):
        raise TypeError("'make_valid' must be a boolean")
    make_valid = bool(make_valid)

    warn_crs_mismatch(df1.crs, df2.crs)
    left = _normalize_frame(df1, "left")
    right = _normalize_frame(df2, "right")
    summary = _input_summary(left, right)

    for prefix in ("left", "right"):
        if getattr(summary, f"{prefix}_family_count") > 1:
            number = 1 if prefix == "left" else 2
            raise NotImplementedError(f"df{number} contains mixed geometry types.")

    left_all_polygons = summary.left_row_count == summary.left_polygon_count
    right_all_polygons = summary.right_row_count == summary.right_polygon_count
    invalid_counts = {
        "left": summary.left_invalid_count if left_all_polygons else 0,
        "right": summary.right_invalid_count if right_all_polygons else 0,
    }
    invalid_total = invalid_counts["left"] + invalid_counts["right"]
    if not make_valid and invalid_total:
        raise ValueError(
            "You have passed make_valid=False along with "
            f"{invalid_total} invalid input geometries. Use make_valid=True "
            "or make sure that all geometries are valid before using overlay."
        )
    if make_valid:
        left = _repair_frame(left, invalid_counts["left"])
        right = _repair_frame(right, invalid_counts["right"])

    pairs = _candidate_pairs(left, right)
    common_layout = _common_layout(left, right, how) if how != "difference" else None
    intersection = None
    left_difference = None
    right_difference = None

    if how in ("intersection", "identity", "union"):
        assert common_layout is not None
        intersection = _intersection_result(pairs, left, right, common_layout)

    if how in ("difference", "identity", "symmetric_difference", "union"):
        left_rows = _difference_rows(left, right, pairs, "left")
        if how == "difference":
            layout = _difference_layout(left)
            result_sdf = _standalone_difference_result(left_rows, left, layout)
        else:
            assert common_layout is not None
            left_difference = _common_difference_result(
                left_rows, left, common_layout, source_is_left=True
            )

    if how in ("symmetric_difference", "union"):
        assert common_layout is not None
        right_rows = _difference_rows(right, left, pairs, "right")
        right_difference = _common_difference_result(
            right_rows, right, common_layout, source_is_left=False
        )

    if how == "intersection":
        assert common_layout is not None
        layout = common_layout
        result_sdf = intersection
    elif how == "identity":
        assert common_layout is not None
        layout = common_layout
        result_sdf = intersection.unionByName(left_difference)
    elif how == "symmetric_difference":
        assert common_layout is not None
        layout = common_layout
        result_sdf = left_difference.unionByName(right_difference)
    elif how == "union":
        assert common_layout is not None
        layout = common_layout
        result_sdf = intersection.unionByName(left_difference).unionByName(
            right_difference
        )

    if keep_geom_type is not False:
        family = _family_from_type(summary.left_first_type)
        if family is not None:
            result_sdf = _keep_geometry_family(result_sdf, layout, family)
        elif summary.left_row_count:
            raise TypeError(f"`geom_type` does not support {summary.left_first_type}.")

    left_crs = df1.crs
    output_srid = (left_crs.to_epsg() or 0) if left_crs is not None else 0
    return _finalize(result_sdf, layout, left, output_srid)


__all__ = ["overlay"]
