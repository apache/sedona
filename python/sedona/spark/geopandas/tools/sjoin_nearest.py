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

"""Point nearest joins using Sedona's existing KNN execution strategies."""

import math
import numbers
import warnings

import pyspark.pandas as ps
from pyspark.pandas.internal import InternalField, InternalFrame
from pyspark.pandas.utils import scol_for
from pyspark.sql import functions as F

from sedona.spark.geopandas import GeoDataFrame
from sedona.spark.geopandas._crs import warn_crs_mismatch, with_crs_metadata
from sedona.spark.sql import st_functions as stf
from sedona.spark.sql.types import GeometryType


def _prepare_side(frame, prefix):
    """Project physical aliases and check point inputs without collecting geometries."""
    internal = frame._internal
    position = frame._active_geometry_position()
    geometry_name = f"{prefix}_{position}"
    columns = []
    for i, (column, field) in enumerate(
        zip(internal.data_spark_columns, internal.data_fields)
    ):
        # Resolve source CRS before filtering: an empty result cannot infer it from SRIDs.
        if isinstance(field.spark_type, GeometryType) or i == position:
            field = with_crs_metadata(field, frame[internal.column_labels[i][0]].crs)
        columns.append(column.alias(f"{prefix}_{i}", metadata=field.metadata))
    projected = internal.spark_frame.select(
        internal.index_spark_columns[0].alias(f"{prefix}_index"), *columns
    )
    geometry = F.col(geometry_name)
    usable = geometry.isNotNull() & ~stf.ST_IsEmpty(geometry)
    stats = projected.agg(
        F.max(F.when(usable, 1).otherwise(0)).alias("has_points"),
        F.max(
            F.when(usable & (stf.ST_GeometryType(geometry) != "ST_Point"), 1).otherwise(
                0
            )
        ).alias("nonpoint"),
    ).first()
    if stats.nonpoint:
        raise NotImplementedError(
            "sjoin_nearest currently supports only Point geometries"
        )
    return projected.filter(usable), geometry_name, bool(stats.has_points)


def sjoin_nearest(
    left_df,
    right_df,
    how="inner",
    max_distance=None,
    lsuffix="left",
    rsuffix="right",
    distance_col=None,
    exclusive=False,
):
    """Join each left point to its nearest right point using planar distance.

    This initial implementation supports point inputs, ``how='inner'``,
    ``exclusive=False``, default suffixes, unique string column names and
    single-level indexes (including duplicate index values). Null and empty
    geometries do not match. The left geometry, CRS and index are retained.

    Parameters
    ----------
    left_df, right_df : GeoDataFrame
        Distributed point frames to join.
    how : str, default 'inner'
        Only inner joins are supported.
    max_distance : float, optional
        Positive finite distance in CRS units. Filters nearest results; it
        does not reduce the KNN search radius.
    lsuffix, rsuffix : str, default 'left', 'right'
        Only the default suffixes are supported for overlapping columns.
    distance_col : str, optional
        New output column for the distance. Must not replace an input column.
    exclusive : bool, default False
        Only False is supported; equal points can match at distance zero.

    Returns
    -------
    GeoDataFrame
        Nearest matches, with no guaranteed row order.

    Notes
    -----
    Ties follow ``spark.sedona.join.knn.includeTieBreakers`` (default False).
    False chooses one nearest candidate with unspecified tie selection. True
    includes ties, but Sedona may omit distinct rows with identical candidate
    geometries. Unlike GeoPandas, all equidistant rows are not guaranteed.
    This function does not change that session setting.

    Distances are planar and ignore Z. Use a projected CRS for meaningful
    distance units. Scalar validation summaries are evaluated eagerly on
    Spark; input geometry rows remain distributed.
    """
    for name, frame in (("left_df", left_df), ("right_df", right_df)):
        if not isinstance(frame, GeoDataFrame):
            raise ValueError(f"'{name}' should be a GeoDataFrame")
        internal = frame._internal
        if len(internal.index_spark_columns) != 1:
            raise NotImplementedError("sjoin_nearest requires a single-level index")
        labels = internal.column_labels
        if any(len(label) != 1 or not isinstance(label[0], str) for label in labels):
            raise NotImplementedError("sjoin_nearest requires flat string columns")
        if len(set(labels)) != len(labels):
            raise NotImplementedError("sjoin_nearest requires unique columns")
        if frame._active_geometry_position() is None:
            raise ValueError(f"'{name}' has no active geometry column")
    if how != "inner":
        raise NotImplementedError("sjoin_nearest currently supports only how='inner'")
    if exclusive is not False:
        raise NotImplementedError(
            "sjoin_nearest currently supports only exclusive=False"
        )
    if lsuffix != "left" or rsuffix != "right":
        raise NotImplementedError(
            "sjoin_nearest currently supports only default suffixes"
        )
    if max_distance is not None and (
        isinstance(max_distance, bool)
        or not isinstance(max_distance, numbers.Real)
        or not math.isfinite(max_distance)
        or max_distance <= 0
    ):
        raise ValueError("max_distance must be a positive finite number")
    if distance_col is not None and (
        not isinstance(distance_col, str)
        or distance_col in left_df.columns
        or distance_col in right_df.columns
    ):
        raise ValueError("distance_col must be a new string column name")

    left_labels = list(left_df.columns)
    right_labels = [
        label for label in right_df.columns if label != right_df.active_geometry_name
    ]
    right_index_name = right_df._internal.index_names[0]
    right_index_label = (
        right_index_name[0] if right_index_name is not None else "index_right"
    )
    if not isinstance(right_index_label, str):
        raise NotImplementedError(
            "sjoin_nearest requires a string or unnamed right index"
        )
    if right_index_label in right_labels or (
        right_index_name is None and right_index_label in left_labels
    ):
        raise ValueError(f"'{right_index_label}' conflicts with a join column")
    right_labels.insert(0, right_index_label)
    left_index_name = left_df._internal.index_names[0]
    left_index_label = (
        left_index_name[0] if left_index_name is not None else "index_left"
    )
    if left_index_label in left_labels or (
        left_index_name is None and left_index_label in right_labels
    ):
        raise ValueError(f"'{left_index_label}' conflicts with a join column")
    overlap = (set(left_labels) | {left_index_label}).intersection(right_labels)
    result_index_name = (
        (f"{left_index_label}_left",)
        if left_index_label in overlap and left_index_name is not None
        else left_index_name
    )
    left_output = [
        (
            label + "_left"
            if label in overlap and label != left_df.active_geometry_name
            else label
        )
        for label in left_labels
    ]
    right_output = [
        label + "_right" if label in overlap else label for label in right_labels
    ]
    output_labels = left_output + right_output
    if len(set(output_labels)) != len(output_labels):
        raise ValueError("sjoin_nearest suffixes create duplicate output columns")
    if distance_col in output_labels:
        raise ValueError("distance_col conflicts with a join output column")

    left_crs, right_crs = left_df.crs, right_df.crs
    warn_crs_mismatch(left_crs, right_crs)
    if any(crs is not None and crs.is_geographic for crs in (left_crs, right_crs)):
        warnings.warn(
            "sjoin_nearest uses planar distances in a geographic CRS; "
            "use to_crs() to project the geometries before joining.",
            UserWarning,
            stacklevel=2,
        )
    left, left_geometry, left_has_points = _prepare_side(left_df, "__nearest_l")
    right, right_geometry, right_has_points = _prepare_side(right_df, "__nearest_r")
    if left_has_points and right_has_points:
        joined = left.join(
            right,
            F.expr(f"ST_KNN({left_geometry}, {right_geometry}, 1, false)"),
            "inner",
        )
    else:
        # Existing KNN partitioning needs a non-empty extent on both sides.
        # A false join builds the correct empty schema without invoking KNN.
        joined = left.limit(0).join(right.limit(0), F.lit(False), "inner")
    if distance_col is not None or max_distance is not None:
        distance = stf.ST_Distance(F.col(left_geometry), F.col(right_geometry))
        joined = joined.withColumn("__nearest_distance", distance)
        if max_distance is not None:
            # A non-deterministic guard keeps this filter above the join.
            # Partition IDs are always nonnegative. Without the guard Spark
            # pushes the bound into the join: Sedona may choose a distance
            # join instead of KNN, or discard it in a broadcast KNN plan.
            # Extra-condition loss is tracked in GH-3398.
            joined = joined.filter(
                F.when(
                    F.spark_partition_id() >= 0,
                    F.col("__nearest_distance") <= float(max_distance),
                ).otherwise(False)
            )

    output_names = [f"__nearest_l_{i}" for i in range(len(left_labels))]
    output_names += ["__nearest_r_index"] + [
        f"__nearest_r_{i}"
        for i, label in enumerate(right_df.columns)
        if label != right_df.active_geometry_name
    ]
    if distance_col is not None:
        output_names.append("__nearest_distance")
        output_labels.append(distance_col)
    result = joined.select("__nearest_l_index", *output_names)
    internal = InternalFrame(
        spark_frame=result,
        index_spark_columns=[scol_for(result, "__nearest_l_index")],
        index_names=[result_index_name],
        column_labels=[(label,) for label in output_labels],
        data_spark_columns=[scol_for(result, name) for name in output_names],
        data_fields=[
            InternalField.from_struct_field(result.schema[name])
            for name in output_names
        ],
    )
    return GeoDataFrame(ps.DataFrame(internal), geometry=left_df.active_geometry_name)
