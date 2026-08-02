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

"""Distributed coordinate subsetting and clipping helpers."""

from __future__ import annotations

import numbers
import typing
import warnings

import geopandas as gpd
import numpy as np
from pandas.api.types import is_list_like
from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame
from pyspark.pandas.internal import NATURAL_ORDER_COLUMN_NAME
from pyspark.pandas.series import first_series
from pyspark.pandas.utils import scol_for, verify_temp_column_name
from pyspark.sql import functions as F
from shapely.geometry import MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry

from sedona.spark.sql import st_aggregates as sta
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf
from sedona.spark.sql import st_predicates as stp

_POINT_TYPES = ("ST_Point", "ST_MultiPoint")
_LINE_TYPES = ("ST_LineString", "ST_MultiLineString")
_POLYGON_TYPES = ("ST_Polygon", "ST_MultiPolygon")
_GEOMETRY_COLLECTION = "ST_GeometryCollection"


def _temporary_column_name(sdf, base: str, reserved: set[str]) -> str:
    """Return a Spark column name that cannot collide with user columns."""
    suffix = 0
    candidate = f"__geopandas_{base}__"
    while candidate in reserved:
        suffix += 1
        candidate = f"__geopandas_{base}_{suffix}__"
    reserved.add(candidate)
    return typing.cast(str, verify_temp_column_name(sdf, candidate))


def _geometry_context(obj):
    """Resolve an object's Spark frame and active physical geometry column."""
    internal = obj._internal.resolved_copy
    geometry = obj.geometry
    geometry_internal = geometry._internal.resolved_copy
    geometry_name = geometry_internal.data_spark_column_names[0]
    return internal, internal.spark_frame, geometry_name


def _rebuild_like(obj, internal, sdf):
    """Rebuild a distributed GeoSeries/GeoDataFrame from a projected frame."""
    from sedona.spark.geopandas.geodataframe import GeoDataFrame
    from sedona.spark.geopandas.geoseries import GeoSeries

    result_internal = internal.copy(
        spark_frame=sdf,
        index_spark_columns=[
            scol_for(sdf, name) for name in internal.index_spark_column_names
        ],
        data_spark_columns=[
            scol_for(sdf, name) for name in internal.data_spark_column_names
        ],
    )

    if isinstance(obj, GeoSeries):
        return GeoSeries(first_series(PandasOnSparkDataFrame(result_internal)))

    result = GeoDataFrame(PandasOnSparkDataFrame(result_internal))
    result._geometry_column_name = obj.active_geometry_name
    return result


def _project_replaced_geometry(sdf, geometry_name: str, geometry):
    """Select the original columns, replacing only the active geometry."""
    return sdf.select(
        *[
            (geometry.alias(name) if name == geometry_name else scol_for(sdf, name))
            for name in sdf.columns
        ]
    )


def _sort_by_natural_order(sdf):
    """Restore input positional order, matching GeoPandas' sorted index query."""
    return sdf.orderBy(scol_for(sdf, NATURAL_ORDER_COLUMN_NAME).asc()).select(
        *[scol_for(sdf, name) for name in sdf.columns]
    )


def _normalize_slice_bound(value, axis: str):
    if value is None:
        return None
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(
        value, numbers.Real
    ):
        raise TypeError(f"{axis} slice bounds must be numeric or None")
    return float(value)


class _CoordinateIndexer:
    """Coordinate-based indexer shared by GeoSeries and GeoDataFrame."""

    def __init__(self, obj):
        self.obj = obj

    def __getitem__(self, key):
        if not isinstance(key, tuple) or len(key) != 2:
            raise TypeError("Coordinate based indexing requires an x and y slice")

        xs, ys = key
        if type(xs) is not slice:
            xs = slice(xs, xs)
        if type(ys) is not slice:
            ys = slice(ys, ys)

        if xs.step is not None or ys.step is not None:
            warnings.warn(
                "Ignoring step - full interval is used.",
                UserWarning,
                stacklevel=2,
            )

        xmin = _normalize_slice_bound(xs.start, "x")
        xmax = _normalize_slice_bound(xs.stop, "x")
        ymin = _normalize_slice_bound(ys.start, "y")
        ymax = _normalize_slice_bound(ys.stop, "y")

        # Shapely's box constructor, which GeoPandas uses, normalizes reversed
        # slice bounds.
        if xmin is not None and xmax is not None and xmin > xmax:
            xmin, xmax = xmax, xmin
        if ymin is not None and ymax is not None and ymin > ymax:
            ymin, ymax = ymax, ymin

        return _coordinate_subset(self.obj, xmin, ymin, xmax, ymax)


def _coordinate_subset(obj, xmin, ymin, xmax, ymax):
    """Filter rows by exact intersection with a distributed bounding box."""
    internal, source_sdf, geometry_name = _geometry_context(obj)
    reserved = set(source_sdf.columns)
    source_geometry = scol_for(source_sdf, geometry_name)
    bounded_sdf = source_sdf

    bound_values = {
        "xmin": xmin,
        "ymin": ymin,
        "xmax": xmax,
        "ymax": ymax,
    }
    missing_bounds = [name for name, value in bound_values.items() if value is None]

    if missing_bounds:
        aggregate_names = {
            name: _temporary_column_name(source_sdf, f"cx_{name}", reserved)
            for name in missing_bounds
        }
        valid_geometry = source_geometry.isNotNull() & ~stf.ST_IsEmpty(source_geometry)
        aggregate_builders = {
            "xmin": lambda: F.min(F.when(valid_geometry, stf.ST_XMin(source_geometry))),
            "ymin": lambda: F.min(F.when(valid_geometry, stf.ST_YMin(source_geometry))),
            "xmax": lambda: F.max(F.when(valid_geometry, stf.ST_XMax(source_geometry))),
            "ymax": lambda: F.max(F.when(valid_geometry, stf.ST_YMax(source_geometry))),
        }
        bounds_sdf = source_sdf.agg(
            *[
                aggregate_builders[name]().alias(aggregate_names[name])
                for name in missing_bounds
            ]
        )
        bounded_sdf = source_sdf.crossJoin(F.broadcast(bounds_sdf))
        source_geometry = scol_for(bounded_sdf, geometry_name)
        for name in missing_bounds:
            bound_values[name] = scol_for(bounded_sdf, aggregate_names[name])

    envelope = stc.ST_MakeEnvelope(
        bound_values["xmin"],
        bound_values["ymin"],
        bound_values["xmax"],
        bound_values["ymax"],
        stf.ST_SRID(source_geometry),
    )
    filtered = bounded_sdf.where(stp.ST_Intersects(source_geometry, envelope)).select(
        *[scol_for(bounded_sdf, name) for name in source_sdf.columns]
    )
    return _rebuild_like(obj, internal, filtered)


def _normalize_rectangle(mask):
    values = tuple(mask)
    if len(values) != 4:
        raise TypeError(
            "If 'mask' is list-like, it must have four values "
            "(minx, miny, maxx, maxy)"
        )

    normalized = []
    for value in values:
        if isinstance(value, (str, bytes, bytearray)) or not isinstance(
            value, numbers.Real
        ):
            raise TypeError("Rectangle mask values must be numeric")
        normalized.append(float(value))
    return tuple(normalized)


def _mask_is_list_like_rectangle(mask) -> bool:
    from sedona.spark.geopandas.geodataframe import GeoDataFrame
    from sedona.spark.geopandas.geoseries import GeoSeries

    return is_list_like(mask) and not isinstance(
        mask,
        (
            GeoDataFrame,
            GeoSeries,
            gpd.GeoDataFrame,
            gpd.GeoSeries,
            Polygon,
            MultiPolygon,
        ),
    )


def _as_distributed_mask(mask):
    """Convert local GeoPandas masks without collecting distributed masks."""
    from sedona.spark.geopandas.geodataframe import GeoDataFrame
    from sedona.spark.geopandas.geoseries import GeoSeries

    if isinstance(mask, gpd.GeoDataFrame):
        return GeoSeries(mask.geometry, crs=mask.crs)
    if isinstance(mask, gpd.GeoSeries):
        return GeoSeries(mask, crs=mask.crs)
    if isinstance(mask, (GeoDataFrame, GeoSeries)):
        return mask
    return None


def _warn_crs_mismatch(obj, mask):
    if obj.crs == mask.crs:
        return

    try:
        from geopandas.array import _crs_mismatch_warn
    except ImportError:
        warnings.warn(
            f"CRS mismatch between the CRS of left geometries ({obj.crs}) "
            f"and right geometries ({mask.crs}).",
            UserWarning,
            stacklevel=3,
        )
    else:
        _crs_mismatch_warn(obj, mask, stacklevel=3)


def _mask_expression(source_sdf, mask, reserved):
    """Return a source frame and a mask column expression."""
    rectangle = _mask_is_list_like_rectangle(mask)
    if rectangle:
        values = _normalize_rectangle(mask)
        if (
            any(np.isnan(value) for value in values)
            or values[0] >= values[2]
            or values[1] >= values[3]
        ):
            return (
                source_sdf,
                stc.ST_GeomFromWKT(F.lit("POLYGON EMPTY")),
                True,
            )
        return source_sdf, stc.ST_MakeEnvelope(*values), True

    distributed_mask = _as_distributed_mask(mask)
    if distributed_mask is not None:
        _, mask_sdf, mask_geometry_name = _geometry_context(distributed_mask)
        mask_name = _temporary_column_name(source_sdf, "clip_mask", reserved)
        dissolved_mask = mask_sdf.agg(
            sta.ST_Union_Aggr(scol_for(mask_sdf, mask_geometry_name)).alias(mask_name)
        )
        joined = source_sdf.crossJoin(F.broadcast(dissolved_mask))
        return joined, scol_for(joined, mask_name), False

    if isinstance(mask, (Polygon, MultiPolygon)):
        return (
            source_sdf,
            stc.ST_GeomFromWKT(F.lit(mask.wkt)),
            False,
        )

    if isinstance(mask, BaseGeometry):
        raise TypeError(
            "'mask' should be a GeoDataFrame, GeoSeries, " "(Multi)Polygon or list-like"
        )
    raise TypeError(
        "'mask' should be a GeoDataFrame, GeoSeries, "
        f"(Multi)Polygon or list-like, got {type(mask)}"
    )


def _geometry_family(geometry):
    geometry_type = stf.ST_GeometryType(geometry)
    return (
        F.when(geometry_type.isin(*_POINT_TYPES), F.lit("point"))
        .when(geometry_type.isin(*_LINE_TYPES), F.lit("line"))
        .when(geometry_type.isin(*_POLYGON_TYPES), F.lit("polygon"))
    )


def _source_geometry_summary(source_sdf, geometry_name):
    """Build a one-row source geometry-family summary."""
    geometry = scol_for(source_sdf, geometry_name)
    geometry_type = stf.ST_GeometryType(geometry)
    family = _geometry_family(geometry)
    return source_sdf.agg(
        F.max(
            F.when(geometry_type == _GEOMETRY_COLLECTION, F.lit(1)).otherwise(F.lit(0))
        ).alias("source_has_collection"),
        F.countDistinct(family).alias("source_family_count"),
        F.min_by(family, scol_for(source_sdf, NATURAL_ORDER_COLUMN_NAME)).alias(
            "source_family"
        ),
    )


def _clipped_geometry_summary(sdf, geometry_name):
    """Build a one-row clipped geometry-family summary."""
    geometry = scol_for(sdf, geometry_name)
    geometry_type = stf.ST_GeometryType(geometry)
    family = _geometry_family(geometry)
    return sdf.agg(
        F.max(
            F.when(geometry_type == _GEOMETRY_COLLECTION, F.lit(1)).otherwise(F.lit(0))
        ).alias("clipped_has_collection"),
        F.countDistinct(family).alias("clipped_family_count"),
    )


def _geometry_summaries(source_sdf, clipped_sdf, geometry_name):
    """Collect source and clipped type metadata in one Spark action."""
    summary = _source_geometry_summary(source_sdf, geometry_name).crossJoin(
        _clipped_geometry_summary(clipped_sdf, geometry_name)
    )
    return summary.first()


def _keep_only_family(internal, sdf, geometry_name, family, explode):
    """Retain the source line or polygon family, exploding when requested."""
    reserved = set(sdf.columns)
    parent_order_name = _temporary_column_name(sdf, "clip_parent_order", reserved)
    position_name = _temporary_column_name(sdf, "clip_position", reserved)
    geometry_value_name = _temporary_column_name(sdf, "clip_geometry", reserved)
    geometry = scol_for(sdf, geometry_name)
    # GeoPandas explodes the entire result only when clipping introduces a
    # GeometryCollection. Multiple basic families alone trigger filtering.
    geometry_parts = stf.ST_Dump(geometry) if explode else F.array(geometry)

    expanded = sdf.select(
        *[
            scol_for(sdf, name)
            for name in sdf.columns
            if name not in (geometry_name, NATURAL_ORDER_COLUMN_NAME)
        ],
        scol_for(sdf, NATURAL_ORDER_COLUMN_NAME).alias(parent_order_name),
        F.posexplode(geometry_parts).alias(position_name, geometry_value_name),
    )

    allowed_types = {
        "line": _LINE_TYPES,
        "polygon": _POLYGON_TYPES,
    }.get(family)
    # GeoPandas filters only line and polygon sources after exploding. Point
    # sources retain every exploded part.
    if allowed_types is not None:
        expanded = expanded.where(
            stf.ST_GeometryType(scol_for(expanded, geometry_value_name)).isin(
                *allowed_types
            )
        )
    expanded = expanded.select(
        *[
            (
                scol_for(expanded, geometry_value_name).alias(geometry_name)
                if name == geometry_name
                else scol_for(expanded, name)
            )
            for name in internal.spark_frame.columns
            if name != NATURAL_ORDER_COLUMN_NAME
        ],
        scol_for(expanded, parent_order_name),
        scol_for(expanded, position_name),
    )
    sort_columns = [scol_for(expanded, parent_order_name)]
    if explode:
        # GEOS overlay collections place higher-dimensional components first,
        # while JTS may emit them in the opposite order.
        sort_columns.append(stf.ST_Dimension(scol_for(expanded, geometry_name)).desc())
    sort_columns.append(scol_for(expanded, position_name))
    expanded = (
        expanded.orderBy(*sort_columns)
        .withColumn(NATURAL_ORDER_COLUMN_NAME, F.monotonically_increasing_id())
        .drop(parent_order_name, position_name)
    )
    return expanded.select(
        *[scol_for(expanded, name) for name in internal.spark_frame.columns]
    )


def clip(gdf, mask, keep_geom_type: bool = False, sort: bool = False):
    """Clip a distributed GeoSeries or GeoDataFrame to a mask.

    The operation uses native Spark and Sedona expressions. Distributed masks
    are dissolved on the cluster and are never materialized as geometry rows
    in Python.

    Parameters
    ----------
    gdf : GeoDataFrame or GeoSeries
        Distributed vector data to clip.
    mask : GeoDataFrame, GeoSeries, Polygon, MultiPolygon, or list-like
        Polygonal mask. A four-value list-like mask is interpreted as
        ``(minx, miny, maxx, maxy)``.
    keep_geom_type : bool, default False
        Retain only the input geometry family when clipping creates lower
        dimensional geometries. Matching GeoPandas' warnings and filtering
        trigger requires one eager metadata aggregation; geometry rows remain
        distributed.
    sort : bool, default False
        Return matching rows in their original positional order.

    Returns
    -------
    GeoDataFrame or GeoSeries
        Clipped distributed data of the same type as ``gdf``.

    See Also
    --------
    GeoDataFrame.clip
    GeoSeries.clip
    """
    from sedona.spark.geopandas.geodataframe import GeoDataFrame
    from sedona.spark.geopandas.geoseries import GeoSeries

    if not isinstance(gdf, (GeoDataFrame, GeoSeries)):
        raise TypeError(f"'gdf' should be GeoDataFrame or GeoSeries, got {type(gdf)}")
    if not isinstance(keep_geom_type, (bool, np.bool_)):
        raise TypeError("'keep_geom_type' must be a boolean")
    if not isinstance(sort, (bool, np.bool_)):
        raise TypeError("'sort' must be a boolean")

    distributed_mask = _as_distributed_mask(mask)
    if distributed_mask is not None:
        _warn_crs_mismatch(gdf, distributed_mask)
        mask = distributed_mask

    internal, source_sdf, geometry_name = _geometry_context(gdf)
    reserved = set(source_sdf.columns)

    working_sdf, mask_geometry, rectangle = _mask_expression(source_sdf, mask, reserved)
    source_geometry = scol_for(working_sdf, geometry_name)
    geometry_type = stf.ST_GeometryType(source_geometry)
    clipped_geometry = F.when(geometry_type == "ST_Point", source_geometry).otherwise(
        stf.ST_SetSRID(
            stf.ST_Intersection(source_geometry, mask_geometry),
            stf.ST_SRID(source_geometry),
        )
    )

    clipped_sdf = working_sdf.where(stp.ST_Intersects(source_geometry, mask_geometry))
    clipped_sdf = _project_replaced_geometry(
        clipped_sdf, geometry_name, clipped_geometry
    )
    clipped_sdf = clipped_sdf.select(
        *[scol_for(clipped_sdf, name) for name in source_sdf.columns]
    )

    if rectangle:
        clipped_sdf = clipped_sdf.where(
            ~stf.ST_IsEmpty(scol_for(clipped_sdf, geometry_name))
        )

    if keep_geom_type:
        summary = _geometry_summaries(source_sdf, clipped_sdf, geometry_name)
        source_family = summary.source_family
        source_supports_keep_geom_type = True
        if summary.source_has_collection:
            warnings.warn(
                "keep_geom_type can not be called on a "
                "GeoDataFrame with GeometryCollection.",
                UserWarning,
                stacklevel=2,
            )
            source_supports_keep_geom_type = False
        elif summary.source_family_count > 1:
            warnings.warn(
                "keep_geom_type can not be called on a mixed type GeoDataFrame.",
                UserWarning,
                stacklevel=2,
            )
            source_supports_keep_geom_type = False

        # GeoPandas filters only when clipping introduces a collection or an
        # additional basic family. If every line collapses to a point, for
        # example, those points remain even with keep_geom_type=True.
        explode = bool(summary.clipped_has_collection)
        filter_family = source_family is not None and summary.clipped_family_count > 1
        if source_supports_keep_geom_type and (explode or filter_family):
            clipped_sdf = _keep_only_family(
                internal,
                clipped_sdf,
                geometry_name,
                source_family,
                explode=explode,
            )

    if sort:
        clipped_sdf = _sort_by_natural_order(clipped_sdf)

    return _rebuild_like(gdf, internal, clipped_sdf)


__all__ = ["clip"]
