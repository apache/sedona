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

"""Distributed geometry-array construction helpers."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import pyspark.pandas as pspd
from pyproj import CRS
from pyspark.pandas.frame import DataFrame as PandasOnSparkDataFrame
from pyspark.pandas.internal import (
    InternalField,
    NATURAL_ORDER_COLUMN_NAME,
    SPARK_DEFAULT_SERIES_NAME,
)
from pyspark.pandas.series import first_series
from pyspark.pandas.utils import same_anchor, scol_for
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    NullType,
    NumericType,
    StructField,
)

from sedona.spark.geopandas._crs import with_crs_metadata
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf
from sedona.spark.sql.types import GeometryType


def _is_distributed_series(value: Any) -> bool:
    return isinstance(value, pspd.Series)


def _coordinate_expression(series: pspd.Series, label: str):
    spark_type = series.spark.data_type
    if not isinstance(spark_type, (NumericType, BooleanType, NullType)):
        raise TypeError(
            f"{label} must be a numeric pandas-on-Spark Series, "
            f"got {spark_type.simpleString()}"
        )

    # GeoPandas converts missing coordinates to IEEE NaN before constructing
    # points. Preserve that distinction from a missing geometry.
    return F.coalesce(series.spark.column.cast("double"), F.lit(float("nan")))


def _coordinate_scalar_expression(value: Any, label: str):
    try:
        array = np.asarray(value, dtype="float64")
    except (TypeError, ValueError, OverflowError) as exc:
        raise TypeError(
            f"{label} must be a numeric scalar or pandas-on-Spark Series"
        ) from exc
    if array.ndim != 0:
        raise TypeError(
            f"{label} must be a numeric scalar when another coordinate is "
            "distributed"
        )
    return F.lit(float(array))


def _distributed_points_from_xy(
    coordinates: list[Any],
    labels: list[str],
    crs: Any | None,
    name: Any | None,
):
    distributed_coordinates = [
        coordinate for coordinate in coordinates if _is_distributed_series(coordinate)
    ]
    source = distributed_coordinates[0]
    if not all(
        same_anchor(source, coordinate) for coordinate in distributed_coordinates[1:]
    ):
        raise ValueError(
            "pandas-on-Spark coordinate Series must share the same distributed "
            "frame and index; align them in one DataFrame before calling "
            "points_from_xy"
        )

    expressions = [
        (
            _coordinate_expression(coordinate, label)
            if _is_distributed_series(coordinate)
            else _coordinate_scalar_expression(coordinate, label)
        )
        for coordinate, label in zip(coordinates, labels)
    ]
    point = (
        stc.ST_PointZ(*expressions)
        if len(expressions) == 3
        else stc.ST_Point(*expressions)
    )
    normalized_crs = CRS.from_user_input(crs) if crs is not None else None
    if normalized_crs is not None:
        point = stf.ST_SetSRID(point, normalized_crs.to_epsg() or 0)

    source_internal = source._internal
    source_sdf = source_internal.spark_frame
    result_name = SPARK_DEFAULT_SERIES_NAME
    result_template = with_crs_metadata(
        InternalField(
            np.dtype("object"),
            StructField(result_name, GeometryType(), nullable=True),
        ),
        normalized_crs,
    )
    result_sdf = source_sdf.select(
        point.alias(result_name, metadata=result_template.metadata),
        *source_internal.index_spark_columns,
        scol_for(source_sdf, NATURAL_ORDER_COLUMN_NAME),
    )
    result_internal = source_internal.copy(
        spark_frame=result_sdf,
        index_spark_columns=[
            scol_for(result_sdf, column_name)
            for column_name in source_internal.index_spark_column_names
        ],
        column_labels=[(result_name,)],
        data_spark_columns=[scol_for(result_sdf, result_name)],
        data_fields=[InternalField(np.dtype("object"), result_sdf.schema[result_name])],
        column_label_names=[None],
    )

    from sedona.spark.geopandas.geoseries import GeoSeries

    result = first_series(PandasOnSparkDataFrame(result_internal)).rename(name)
    return GeoSeries(result)


def _local_coordinate_array(value: Any, label: str) -> np.ndarray:
    try:
        array = np.asarray(value, dtype="float64")
    except (TypeError, ValueError, OverflowError) as exc:
        raise TypeError(f"{label} must be an iterable of numeric values") from exc
    if array.ndim > 1:
        raise ValueError(f"{label} must be one-dimensional")
    return array


def _local_points_from_xy(
    coordinates: list[Any],
    labels: list[str],
    crs: Any | None,
    index,
    name: Any | None,
):
    arrays = [
        _local_coordinate_array(coordinate, label)
        for coordinate, label in zip(coordinates, labels)
    ]
    try:
        arrays = list(np.broadcast_arrays(*arrays))
    except ValueError as exc:
        lengths = ", ".join(
            f"{label}={array.size}" for label, array in zip(labels, arrays)
        )
        raise ValueError(
            f"Coordinate lengths are not broadcast-compatible: {lengths}"
        ) from exc
    if arrays[0].ndim == 0:
        raise TypeError("at least one coordinate must be an iterable")

    if index is None and all(isinstance(value, pd.Series) for value in coordinates):
        first_index = coordinates[0].index
        if all(value.index.equals(first_index) for value in coordinates[1:]):
            index = first_index

    frame = pd.DataFrame(
        {label: array for label, array in zip(labels, arrays)},
        index=index,
    )
    distributed = pspd.from_pandas(frame)
    return _distributed_points_from_xy(
        [distributed[label] for label in labels],
        labels,
        crs,
        name,
    )


def _points_from_xy(
    x,
    y,
    z=None,
    crs=None,
    *,
    index=None,
    name=None,
):
    coordinates = [x, y] if z is None else [x, y, z]
    labels = ["x", "y"] if z is None else ["x", "y", "z"]
    distributed = [_is_distributed_series(value) for value in coordinates]

    if any(distributed):
        if index is not None:
            raise TypeError(
                "index cannot be supplied with distributed coordinate Series; "
                "their shared index is preserved automatically"
            )
        return _distributed_points_from_xy(coordinates, labels, crs, name)

    return _local_points_from_xy(coordinates, labels, crs, index, name)


def points_from_xy(x, y, z=None, crs=None):
    """
    Construct a distributed GeoSeries of Point geometries from coordinates.

    Unlike :func:`geopandas.points_from_xy`, which returns an in-memory
    ``GeometryArray``, this scalable counterpart returns a distributed
    :class:`GeoSeries` so that its index and CRS metadata can be retained.

    Parameters
    ----------
    x, y, z : iterable or pandas-on-Spark Series
        Coordinate values. Distributed Series must come from the same frame;
        numeric scalar coordinates are broadcast over that frame.
    crs : value, optional
        Coordinate Reference System accepted by
        :meth:`pyproj.CRS.from_user_input`.

    Returns
    -------
    GeoSeries
        Distributed Point geometries. Shared distributed indexes, including
        MultiIndexes and duplicate labels, are preserved.

    Examples
    --------
    >>> import sedona.spark.geopandas as sgpd
    >>> points = sgpd.points_from_xy([1, 2], [3, 4], crs="EPSG:4326")
    >>> points
    0    POINT (1 3)
    1    POINT (2 4)
    dtype: geometry

    Notes
    -----
    Construction uses native ``ST_Point`` or ``ST_PointZ`` expressions. It
    does not collect pandas-on-Spark inputs or execute a Python row UDF.

    Local coordinates use NumPy broadcasting and are paired positionally, as
    in GeoPandas. Local iterables, NumPy arrays, and pandas objects are
    materialized on the driver and are intended for bounded inputs. Use
    pandas-on-Spark Series for large coordinate columns.

    Distributed Series are paired within their shared Spark plan and retain
    that plan's index. Series from unrelated plans are rejected because they
    have no safe distributed positional relationship.
    """
    return _points_from_xy(x, y, z=z, crs=crs)
