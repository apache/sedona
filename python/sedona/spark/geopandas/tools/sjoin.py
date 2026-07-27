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
import numbers
import re
import warnings
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

import numpy as np
import pyspark.pandas as ps
from pyspark.pandas.internal import (
    NATURAL_ORDER_COLUMN_NAME,
    SPARK_DEFAULT_INDEX_NAME,
    InternalFrame,
)
from pyspark.pandas.utils import scol_for
from pyspark.sql import Column, DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import expr

from sedona.spark.geopandas import GeoDataFrame, GeoSeries
from sedona.spark.sql import st_functions as stf
from sedona.spark.sql import st_predicates as stp

# Pre-compiled regex pattern for suffix validation.
SUFFIX_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


@dataclass(frozen=True)
class _NearestJoinSide:
    """A join input projected onto collision-proof Spark column names."""

    spark_frame: SparkDataFrame
    index_aliases: Tuple[str, ...]
    index_names: Tuple[Any, ...]
    data_aliases: Tuple[str, ...]
    data_labels: Tuple[Tuple[Any, ...], ...]
    active_label: Tuple[Any, ...]
    active_name: Any
    geometry_alias: str
    order_alias: str


def _qualified_column(qualifier: str, name: str) -> Column:
    return F.col(f"{qualifier}.`{name}`")


def _prepare_nearest_side(frame: GeoDataFrame, prefix: str) -> _NearestJoinSide:
    internal = frame._internal
    active_label = frame.geometry._column_label
    if active_label is None:
        raise ValueError("GeoDataFrame geometry column is not set")

    try:
        geometry_position = internal.column_labels.index(active_label)
    except ValueError as exc:
        raise ValueError("GeoDataFrame geometry column is not present") from exc

    index_aliases = tuple(
        f"__sjoin_nearest_{prefix}_index_{position}__"
        for position in range(len(internal.index_spark_columns))
    )
    data_aliases = tuple(
        f"__sjoin_nearest_{prefix}_data_{position}__"
        for position in range(len(internal.data_spark_columns))
    )
    order_alias = f"__sjoin_nearest_{prefix}_order__"

    projected = internal.spark_frame.select(
        *[
            column.alias(alias)
            for column, alias in zip(internal.index_spark_columns, index_aliases)
        ],
        *[
            column.alias(alias)
            for column, alias in zip(internal.data_spark_columns, data_aliases)
        ],
        scol_for(internal.spark_frame, NATURAL_ORDER_COLUMN_NAME).alias(order_alias),
    )
    return _NearestJoinSide(
        spark_frame=projected,
        index_aliases=index_aliases,
        index_names=tuple(internal.index_names),
        data_aliases=data_aliases,
        data_labels=tuple(internal.column_labels),
        active_label=active_label,
        active_name=frame.active_geometry_name,
        geometry_alias=data_aliases[geometry_position],
        order_alias=order_alias,
    )


def _nearest_reset_index_labels(
    side: _NearestJoinSide,
    suffix: str,
    other: _NearestJoinSide,
) -> List[Tuple[Any, ...]]:
    """Return GeoPandas-compatible labels for reset index levels."""

    labels: List[Tuple[Any, ...]] = []
    multiple_levels = len(side.index_names) > 1
    for position, name in enumerate(side.index_names):
        if name is None:
            generated = (
                f"index_{suffix}{position}" if multiple_levels else f"index_{suffix}"
            )
            label = (generated,)
            if label in side.data_labels or label in other.data_labels:
                raise ValueError(
                    f"'{generated}' cannot be a column name in the frames being joined"
                )
        else:
            label = name
            if label in side.data_labels:
                display_name = label[0] if len(label) == 1 else label
                raise ValueError(f"cannot insert {display_name}, already exists")
        labels.append(label)
    return labels


def _nearest_suffix_label(label: Tuple[Any, ...], suffix: str) -> Tuple[Any, ...]:
    if len(label) == 1:
        return (f"{label[0]}_{suffix}",)
    return (*label[:-1], f"{label[-1]}_{suffix}")


def _nearest_output_columns(
    left: _NearestJoinSide,
    right: _NearestJoinSide,
    how: str,
    lsuffix: str,
    rsuffix: str,
) -> List[Tuple[str, Tuple[Any, ...]]]:
    """Resolve output values and labels using GeoPandas reset/suffix rules."""

    left_index_labels = _nearest_reset_index_labels(left, lsuffix, right)
    right_index_labels = _nearest_reset_index_labels(right, rsuffix, left)

    left_data = [
        (alias, label)
        for alias, label in zip(left.data_aliases, left.data_labels)
        if how != "right" or alias != left.geometry_alias
    ]
    right_data = [
        (alias, label)
        for alias, label in zip(right.data_aliases, right.data_labels)
        if how == "right" or alias != right.geometry_alias
    ]

    left_reset_labels = left_index_labels + [label for _, label in left_data]
    right_reset_labels = right_index_labels + [label for _, label in right_data]
    overlap = set(left_reset_labels).intersection(right_reset_labels)

    def rename(
        label: Tuple[Any, ...],
        suffix: str,
        geometry_label: Tuple[Any, ...],
    ) -> Tuple[Any, ...]:
        if label in overlap and label != geometry_label:
            return _nearest_suffix_label(label, suffix)
        return label

    output: List[Tuple[str, Tuple[Any, ...]]] = []
    if how == "right":
        output.extend(
            (alias, rename(label, lsuffix, left.active_label))
            for alias, label in zip(left.index_aliases, left_index_labels)
        )
    output.extend(
        (alias, rename(label, lsuffix, left.active_label)) for alias, label in left_data
    )

    if how in ("inner", "left"):
        output.extend(
            (alias, rename(label, rsuffix, right.active_label))
            for alias, label in zip(right.index_aliases, right_index_labels)
        )
    output.extend(
        (alias, rename(label, rsuffix, right.active_label))
        for alias, label in right_data
    )
    return output


def _nearest_result_index_names(
    left: _NearestJoinSide,
    right: _NearestJoinSide,
    how: str,
    lsuffix: str,
    rsuffix: str,
) -> List[Any]:
    """Apply the reset/merge suffix rules to the retained index names."""

    left_index_labels = _nearest_reset_index_labels(left, lsuffix, right)
    right_index_labels = _nearest_reset_index_labels(right, rsuffix, left)
    left_data_labels = [
        label
        for alias, label in zip(left.data_aliases, left.data_labels)
        if how != "right" or alias != left.geometry_alias
    ]
    right_data_labels = [
        label
        for alias, label in zip(right.data_aliases, right.data_labels)
        if how == "right" or alias != right.geometry_alias
    ]
    overlap = set(left_index_labels + left_data_labels).intersection(
        right_index_labels + right_data_labels
    )

    if how in ("inner", "left"):
        side, labels, suffix = left, left_index_labels, lsuffix
    else:
        side, labels, suffix = right, right_index_labels, rsuffix

    return [
        (
            _nearest_suffix_label(label, suffix)
            if name is not None and label in overlap
            else name
        )
        for name, label in zip(side.index_names, labels)
    ]


def _check_sjoin_nearest_crs(left_df: GeoDataFrame, right_df: GeoDataFrame) -> None:
    left_crs = left_df.crs
    right_crs = right_df.crs

    if left_crs != right_crs:
        left_crs_text = "None" if left_crs is None else left_crs.to_string()
        right_crs_text = "None" if right_crs is None else right_crs.to_string()
        warnings.warn(
            "CRS mismatch between the CRS of left geometries and the CRS of "
            "right geometries.\nUse `to_crs()` to reproject one of the input "
            "geometries to match the CRS of the other.\n\n"
            f"Left CRS: {left_crs_text}\n"
            f"Right CRS: {right_crs_text}\n",
            UserWarning,
            stacklevel=3,
        )

    geographic_warning = (
        "Geometry is in a geographic CRS. Results from 'sjoin_nearest' are "
        "likely incorrect. Use 'GeoSeries.to_crs()' to re-project geometries "
        "to a projected CRS before this operation.\n"
    )
    for crs in (left_crs, right_crs):
        if crs is not None and crs.is_geographic:
            warnings.warn(
                geographic_warning,
                UserWarning,
                stacklevel=3,
            )


def _normalize_max_distance(max_distance) -> Optional[float]:
    if max_distance is None:
        return None
    if not isinstance(max_distance, numbers.Real):
        raise TypeError("max_distance must be a number")
    max_distance = float(max_distance)
    if max_distance <= 0:
        raise ValueError("max_distance must be greater than 0")
    return max_distance


def _nearest_frame_join(
    left_df: GeoDataFrame,
    right_df: GeoDataFrame,
    how: str,
    max_distance: Optional[float],
    lsuffix: str,
    rsuffix: str,
    distance_col: Optional[str],
    exclusive: bool,
) -> GeoDataFrame:
    left = _prepare_nearest_side(left_df, "left")
    right = _prepare_nearest_side(right_df, "right")

    if how == "right":
        query, objects = right, left
        left_qualifier, right_qualifier = "o", "q"
    else:
        query, objects = left, right
        left_qualifier, right_qualifier = "q", "o"

    query_frame = query.spark_frame.alias("q")
    object_frame = objects.spark_frame.alias("o")
    query_geometry = _qualified_column("q", query.geometry_alias)
    object_geometry = _qualified_column("o", objects.geometry_alias)
    distance = stf.ST_Distance(query_geometry, object_geometry)
    condition = stp.ST_KNN(
        query_geometry,
        object_geometry,
        1,
        use_spheroid=False,
        include_ties=True,
        exclusive=exclusive,
    )
    if max_distance is not None:
        condition = condition & (distance <= F.lit(max_distance))

    matches = query_frame.join(object_frame, condition, "inner")
    distance_alias = "__sjoin_nearest_distance__"

    if how == "inner":
        joined = matches.select(
            *[
                _qualified_column(left_qualifier, name).alias(name)
                for name in left.spark_frame.columns
            ],
            *[
                _qualified_column(right_qualifier, name).alias(name)
                for name in right.spark_frame.columns
            ],
            *([distance.alias(distance_alias)] if distance_col is not None else []),
        )
    else:
        match_id = "__sjoin_nearest_match_id__"
        payload = matches.select(
            _qualified_column("q", query.order_alias).alias(match_id),
            *[
                _qualified_column("o", name).alias(name)
                for name in objects.spark_frame.columns
            ],
            *([distance.alias(distance_alias)] if distance_col is not None else []),
        )
        joined = query.spark_frame.alias("base").join(
            payload.alias("match"),
            _qualified_column("base", query.order_alias)
            == _qualified_column("match", match_id),
            "left",
        )

    active = left if how in ("inner", "left") else right
    opposite = right if how in ("inner", "left") else left
    joined = joined.orderBy(
        F.col(active.order_alias).asc(),
        F.col(opposite.order_alias).asc_nulls_last(),
    )

    output_columns = _nearest_output_columns(
        left,
        right,
        how,
        lsuffix,
        rsuffix,
    )
    distance_replaces_geometry = False
    if distance_col is not None:
        distance_label = (distance_col,)
        distance_replaces_geometry = distance_label == active.active_label
        replaced = False
        for position, (_, label) in enumerate(output_columns):
            if label == distance_label:
                output_columns[position] = (distance_alias, distance_label)
                replaced = True
                break
        if not replaced:
            output_columns.append((distance_alias, distance_label))

    index_names = [
        f"__sjoin_nearest_result_index_{position}__"
        for position in range(len(active.index_aliases))
    ]
    data_names = [
        f"__sjoin_nearest_result_data_{position}__"
        for position in range(len(output_columns))
    ]
    result_frame = joined.select(
        *[
            F.col(alias).alias(name)
            for alias, name in zip(active.index_aliases, index_names)
        ],
        *[
            F.col(alias).alias(name)
            for (alias, _), name in zip(output_columns, data_names)
        ],
    )

    internal = InternalFrame(
        spark_frame=result_frame,
        index_spark_columns=[scol_for(result_frame, name) for name in index_names],
        index_names=_nearest_result_index_names(
            left,
            right,
            how,
            lsuffix,
            rsuffix,
        ),
        column_labels=[label for _, label in output_columns],
        data_spark_columns=[scol_for(result_frame, name) for name in data_names],
        column_label_names=(
            left_df._internal.column_label_names
            if how in ("inner", "left")
            else right_df._internal.column_label_names
        ),
    )
    result = GeoDataFrame(ps.DataFrame(internal))
    if distance_replaces_geometry:
        warnings.warn(
            "Geometry column does not contain geometry.",
            UserWarning,
            stacklevel=3,
        )
        object.__setattr__(result, "_geometry_column_name", None)
    else:
        object.__setattr__(result, "_geometry_column_name", active.active_name)
        object.__setattr__(
            result,
            "_empty_crs_source",
            left_df.geometry if how in ("inner", "left") else right_df.geometry,
        )
    return result


def _frame_join(
    left_df: GeoDataFrame,
    right_df: GeoDataFrame,
    how="inner",
    predicate="intersects",
    lsuffix="left",
    rsuffix="right",
    distance=None,
    on_attribute=None,
):
    """Join the GeoDataFrames at the DataFrame level.

    Parameters
    ----------
    left_df : GeoDataFrame
        Left dataset to join
    right_df : GeoDataFrame
        Right dataset to join
    how : str, default 'inner'
        Join type: 'inner', 'left', 'right'
    predicate : str, default 'intersects'
        Spatial predicate to use
    lsuffix : str, default 'left'
        Suffix for left overlapping columns
    rsuffix : str, default 'right'
        Suffix for right overlapping columns
    distance : float, optional
        Distance parameter for dwithin predicate
    on_attribute : list, optional
        Additional columns to join on

    Note: Unlike GeoPandas, Sedona does not preserve key order for performance reasons. Consider using .sort_index() after the join, if you need to preserve the order.

    Returns
    -------
    GeoDataFrame or GeoSeries
        Joined result
    """
    # Predicate mapping.
    predicate_map = {
        "intersects": "ST_Intersects",
        "contains": "ST_Contains",
        "within": "ST_Within",
        "touches": "ST_Touches",
        "crosses": "ST_Crosses",
        "overlaps": "ST_Overlaps",
        "dwithin": "ST_DWithin",
        "covers": "ST_Covers",
        "covered_by": "ST_CoveredBy",
        # "contains_properly": "ST_ContainsProperly",  # Not supported by Sedona yet.
    }

    if predicate not in predicate_map:
        raise ValueError(
            f"Predicate '{predicate}' not supported. Available: {list(predicate_map.keys())}"
        )

    spatial_func = predicate_map[predicate]

    # Get the internal Spark DataFrames.
    left_sdf = left_df._internal.spark_frame
    right_sdf = right_df._internal.spark_frame

    # Handle geometry columns - check if they exist and get proper column names.
    left_geom_col = None
    right_geom_col = None

    # Find geometry columns in left DataFrame.
    left_geom_col = left_df.active_geometry_name

    # Find geometry columns in right DataFrame.
    right_geom_col = right_df.active_geometry_name

    if not left_geom_col:
        raise ValueError("Left DataFrame geometry column not set")
    if not right_geom_col:
        raise ValueError("Right DataFrame geometry column not set")

    left_geom_expr = f"`{left_geom_col}` as l_geometry"
    right_geom_expr = f"`{right_geom_col}` as r_geometry"

    # Select all columns with geometry.
    left_cols = [left_geom_expr] + [
        f"`{field.name}` as l_{field.name}"
        for field in left_sdf.schema.fields
        if field.name != left_geom_col and not field.name.startswith("__")
    ]
    right_cols = [right_geom_expr] + [
        f"`{field.name}` as r_{field.name}"
        for field in right_sdf.schema.fields
        if field.name != right_geom_col and not field.name.startswith("__")
    ]

    left_geo_df = left_sdf.selectExpr(
        *left_cols, f"`{SPARK_DEFAULT_INDEX_NAME}` as index_{lsuffix}"
    )
    right_geo_df = right_sdf.selectExpr(
        *right_cols, f"`{SPARK_DEFAULT_INDEX_NAME}` as index_{rsuffix}"
    )

    # Build spatial join condition.
    if predicate == "dwithin":
        if distance is None:
            raise ValueError("Distance parameter is required for 'dwithin' predicate")
        spatial_condition = f"{spatial_func}(l_geometry, r_geometry, {distance})"
    else:
        spatial_condition = f"{spatial_func}(l_geometry, r_geometry)"

    # Add attribute-based join condition if specified.
    join_condition = spatial_condition
    if on_attribute:
        for attr in on_attribute:
            join_condition += f" AND l_{attr} = r_{attr}"

    # Perform spatial join based on join type.
    if how == "inner":
        spatial_join_df = left_geo_df.alias("l").join(
            right_geo_df.alias("r"), expr(join_condition)
        )
    elif how == "left":
        spatial_join_df = left_geo_df.alias("l").join(
            right_geo_df.alias("r"), expr(join_condition), "left"
        )
    elif how == "right":
        spatial_join_df = left_geo_df.alias("l").join(
            right_geo_df.alias("r"), expr(join_condition), "right"
        )
    else:
        raise ValueError(f"Join type '{how}' not supported")

    # Pick which index to use for the resulting df's index based on 'how'.
    index_col = f"index_{lsuffix}" if how in ("inner", "left") else f"index_{rsuffix}"

    # Handle column naming with suffixes.
    final_columns = []

    # Add geometry column (always from left for GeoPandas compatibility).
    final_columns.append("l_geometry as geometry")

    # Add other columns with suffix handling.
    left_data_cols = [
        col
        for col in left_geo_df.columns
        if col not in ["l_geometry", f"index_{lsuffix}"]
    ]
    right_data_cols = [
        col
        for col in right_geo_df.columns
        if col not in ["r_geometry", f"index_{rsuffix}"]
    ]

    final_columns.append(f"{index_col} as {SPARK_DEFAULT_INDEX_NAME}")

    if index_col != f"index_{lsuffix}":
        final_columns.append(f"index_{lsuffix}")

    for col_name in left_data_cols:
        base_name = col_name[2:]  # Remove "l_" prefix
        right_col = f"r_{base_name}"

        if right_col in right_data_cols:
            # Column exists in both - apply suffixes.
            final_columns.append(f"{col_name} as {base_name}_{lsuffix}")
        else:
            # Column only in left.
            final_columns.append(f"{col_name} as {base_name}")

    if index_col != f"index_{rsuffix}":
        final_columns.append(f"index_{rsuffix}")

    for col_name in right_data_cols:
        base_name = col_name[2:]  # Remove "r_" prefix
        left_col = f"l_{base_name}"

        if left_col in left_data_cols:
            # Column exists in both - apply suffixes.
            final_columns.append(f"{col_name} as {base_name}_{rsuffix}")
        else:
            # Column only in right.
            final_columns.append(f"{col_name} as {base_name}")

    # Select final columns.
    result_df = spatial_join_df.selectExpr(*final_columns)
    # Note, we do not .orderBy(SPARK_DEFAULT_INDEX_NAME) to avoid a performance hit.

    data_spark_columns = [
        scol_for(result_df, col)
        for col in result_df.columns
        if col != SPARK_DEFAULT_INDEX_NAME
    ]

    internal = InternalFrame(
        spark_frame=result_df,
        index_spark_columns=[scol_for(result_df, SPARK_DEFAULT_INDEX_NAME)],
        data_spark_columns=data_spark_columns,
    )
    return GeoDataFrame(ps.DataFrame(internal))


def sjoin(
    left_df: GeoDataFrame,
    right_df: GeoDataFrame,
    how="inner",
    predicate="intersects",
    lsuffix="left",
    rsuffix="right",
    distance=None,
    on_attribute=None,
    **kwargs,
) -> GeoDataFrame:
    """Spatial join of two GeoDataFrames.

    Parameters
    ----------
    left_df, right_df : GeoDataFrames
    how : string, default 'inner'
        The type of join:

        * 'left': use keys from left_df; retain only left_df geometry column
        * 'right': use keys from right_df; retain only right_df geometry column
        * 'inner': use intersection of keys from both dfs; retain only
          left_df geometry column
    predicate : string, default 'intersects'
        Binary predicate. Valid values are determined by the spatial index used.
        You can check the valid values in left_df or right_df as
        ``left_df.sindex.valid_query_predicates`` or
        ``right_df.sindex.valid_query_predicates``
        Replaces deprecated ``op`` parameter.
    lsuffix : string, default 'left'
        Suffix to apply to overlapping column names (left GeoDataFrame).
    rsuffix : string, default 'right'
        Suffix to apply to overlapping column names (right GeoDataFrame).
    distance : number or array_like, optional
        Distance(s) around each input geometry within which to query the tree
        for the 'dwithin' predicate. If array_like, must be
        one-dimensional with length equal to length of left GeoDataFrame.
        Required if ``predicate='dwithin'``.
    on_attribute : string, list or tuple
        Column name(s) to join on as an additional join restriction on top
        of the spatial predicate. These must be found in both DataFrames.
        If set, observations are joined only if the predicate applies
        and values in specified columns match.

    Returns
    -------
    GeoDataFrame
        The joined GeoDataFrame.

    Examples
    --------
    >>> from sedona.spark.geopandas.tools import sjoin
    >>> groceries_w_communities = sjoin(groceries, chicago)
    >>> groceries_w_communities.head()  # doctest: +SKIP
       OBJECTID       community                           geometry
    0        16          UPTOWN  MULTIPOINT ((-87.65661 41.97321))
    1        18     MORGAN PARK  MULTIPOINT ((-87.68136 41.69713))
    2        22  NEAR WEST SIDE  MULTIPOINT ((-87.63918 41.86847))
    3        23  NEAR WEST SIDE  MULTIPOINT ((-87.65495 41.87783))
    4        27         CHATHAM  MULTIPOINT ((-87.62715 41.73623))
    [5 rows x 95 columns]

    Notes
    -----
    Every operation in GeoPandas is planar, i.e. the potential third
    dimension is not taken into account.
    """
    if kwargs:
        first = next(iter(kwargs.keys()))
        raise TypeError(f"sjoin() got an unexpected keyword argument '{first}'")

    on_attribute = _maybe_make_list(on_attribute)

    _basic_checks(left_df, right_df, how, lsuffix, rsuffix, on_attribute=on_attribute)

    joined = _frame_join(
        left_df,
        right_df,
        how=how,
        predicate=predicate,
        lsuffix=lsuffix,
        rsuffix=rsuffix,
        distance=distance,
        on_attribute=on_attribute,
    )

    return joined


def sjoin_nearest(
    left_df: GeoDataFrame,
    right_df: GeoDataFrame,
    how: str = "inner",
    max_distance: Optional[float] = None,
    lsuffix: str = "left",
    rsuffix: str = "right",
    distance_col: Optional[str] = None,
    exclusive: bool = False,
) -> GeoDataFrame:
    """Spatial join based on the nearest distributed geometry matches.

    Every equidistant nearest match is returned. The operation is planned as
    Sedona's distributed KNN join and does not collect geometry rows to the
    driver.

    Parameters
    ----------
    left_df, right_df : GeoDataFrame
        Frames to join.
    how : {"inner", "left", "right"}, default "inner"
        Join mode and the side whose index and active geometry are retained.
    max_distance : float, optional
        Maximum planar distance for a nearest match. Must be greater than zero.
    lsuffix, rsuffix : str
        Suffixes applied to overlapping left and right column names.
    distance_col : str, optional
        Output column containing the planar distance between each match.
    exclusive : bool, default False
        Exclude candidates topologically equal to the query geometry before
        nearest matches are ranked.

    Returns
    -------
    GeoDataFrame
        Distributed nearest-join result.

    Examples
    --------
    >>> from shapely.geometry import Point
    >>> from sedona.spark.geopandas import GeoDataFrame, sjoin_nearest
    >>> left = GeoDataFrame(
    ...     {"name": ["a"], "geometry": [Point(0, 0)]}, crs="EPSG:3857"
    ... )
    >>> right = GeoDataFrame(
    ...     {"value": [1, 2], "geometry": [Point(-1, 0), Point(1, 0)]},
    ...     crs="EPSG:3857",
    ... )
    >>> sjoin_nearest(left, right, distance_col="distance").sort_values("value")
      name     geometry  index_right  value  distance
    0    a  POINT (0 0)            0      1       1.0
    0    a  POINT (0 0)            1      2       1.0

    Notes
    -----
    Distances are planar and expressed in CRS units. Geographic CRS inputs
    therefore emit the same accuracy warning as GeoPandas.
    """
    _basic_checks(left_df, right_df, how, lsuffix, rsuffix)

    if left_df.active_geometry_name is None:
        raise ValueError("Left DataFrame geometry column not set")
    if right_df.active_geometry_name is None:
        raise ValueError("Right DataFrame geometry column not set")
    if isinstance(exclusive, np.bool_):
        exclusive = bool(exclusive)
    elif isinstance(exclusive, numbers.Integral):
        if exclusive not in (0, 1):
            raise ValueError("exclusive parameter must be boolean")
        exclusive = bool(exclusive)
    elif not isinstance(exclusive, bool):
        if not np.isscalar(exclusive):
            raise ValueError("exclusive parameter only accepts scalar values")
        raise ValueError("exclusive parameter must be boolean")
    if distance_col is not None and not isinstance(distance_col, str):
        raise TypeError("distance_col must be a string or None")

    max_distance = _normalize_max_distance(max_distance)
    _check_sjoin_nearest_crs(left_df, right_df)
    return _nearest_frame_join(
        left_df,
        right_df,
        how,
        max_distance,
        lsuffix,
        rsuffix,
        distance_col,
        exclusive,
    )


def _maybe_make_list(obj):
    if isinstance(obj, tuple):
        return list(obj)
    if obj is not None and not isinstance(obj, list):
        return [obj]
    return obj


def _basic_checks(left_df, right_df, how, lsuffix, rsuffix, on_attribute=None):
    """Checks the validity of join input parameters.

    `how` must be one of the valid options.
    `'index_'` concatenated with `lsuffix` or `rsuffix` must not already
    exist as columns in the left or right data frames.

    Parameters
    ------------
    left_df : GeoDataFrame or GeoSeries
    right_df : GeoDataFrame or GeoSeries
    how : str, one of 'left', 'right', 'inner'
        join type
    lsuffix : str
        left index suffix
    rsuffix : str
        right index suffix
    on_attribute : list, default None
        list of column names to merge on along with geometry
    """
    if not isinstance(left_df, GeoDataFrame):
        raise ValueError(f"'left_df' should be GeoDataFrame, got {type(left_df)}")

    if not isinstance(right_df, GeoDataFrame):
        raise ValueError(f"'right_df' should be GeoDataFrame, got {type(right_df)}")

    allowed_hows = ["inner", "left", "right"]
    if how not in allowed_hows:
        raise ValueError(f'`how` was "{how}" but is expected to be in {allowed_hows}')

    # Check if on_attribute columns exist in both datasets.
    if on_attribute:
        for attr in on_attribute:
            if hasattr(left_df, "columns") and attr not in left_df.columns:
                raise ValueError(f"Column '{attr}' not found in left dataset")
            if hasattr(right_df, "columns") and attr not in right_df.columns:
                raise ValueError(f"Column '{attr}' not found in right dataset")

    # Check for reserved column names that would conflict.
    if lsuffix == rsuffix:
        raise ValueError("lsuffix and rsuffix cannot be the same")

    # Validate suffix format (should not contain special characters that would break SQL).
    if not SUFFIX_PATTERN.match(lsuffix):
        raise ValueError(f"lsuffix '{lsuffix}' contains invalid characters")
    if not SUFFIX_PATTERN.match(rsuffix):
        raise ValueError(f"rsuffix '{rsuffix}' contains invalid characters")


def _to_geo_series(df: ps.Series) -> GeoSeries:
    """
    Get the first Series from the DataFrame.

    Parameters
    ----------
    df : ps.Series
        The input DataFrame.

    Returns
    -------
    GeoSeries
        The first Series from the DataFrame.
    """
    return GeoSeries(data=df)
