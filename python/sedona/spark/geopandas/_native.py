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

"""Sedona-owned pandas-on-Spark boundary for native spatial columns."""

import numpy as np
import pandas as pd
import pyspark.pandas as ps
import shapely
from geopandas.array import GeometryDtype
from pyproj import CRS
from shapely.geometry.base import BaseGeometry
from pyspark.pandas.internal import InternalField, NATURAL_ORDER_COLUMN_NAME
from pyspark.pandas.utils import scol_for

from sedona.spark.sql.st_constructors import ST_GeomFromWKB
from sedona.spark.sql.st_functions import ST_SetSRID, ST_AsBinary, ST_SRID
from sedona.spark.sql.types import GeometryType
from sedona.spark.geopandas._crs import with_crs_metadata
from pyspark.sql import functions as F


class NativeScalarSeries(ps.Series):
    """Collect a scalar column without reading native geometry in its anchor."""

    def _to_internal_pandas(self):
        # Keep the shared anchor during distributed operations. Project only
        # when collecting, since pandas-on-Spark cannot restore native geometry.
        return self._internal.to_pandas_frame.iloc[:, 0]

    def __repr__(self):
        return display(self)


class _NativeDisplayFrame(ps.DataFrame):
    def _to_internal_pandas(self):
        return to_pandas(self._internal)

    def _get_or_create_repr_pandas_cache(self, n):
        # Fetch only the display limit plus one row for PySpark's truncation footer.
        return to_pandas(super().head(n + 1)._internal)


class _NativeDisplaySeries(ps.Series):
    def _to_internal_pandas(self):
        return to_pandas(self._internal).iloc[:, 0]


def display(value, html=False):
    """Use PySpark's formatter with a spatial-aware, temporary collection boundary."""
    frame = _NativeDisplayFrame(value._internal)
    if isinstance(value, ps.Series):
        return repr(_NativeDisplaySeries(data=frame, index=value._column_label))
    return frame._repr_html_() if html else repr(frame)


def _prepare_pandas(frame, geometry_columns=None):
    """Prepare WKB/SRID columns, retaining declared geometry even without values."""
    geometry_crs = dict(geometry_columns or {})
    for position, dtype in enumerate(frame.dtypes):
        if isinstance(dtype, GeometryDtype):
            geometry_crs.setdefault(position, frame.iloc[:, position].array.crs)
    local = frame.copy()
    original_count = len(local.columns)
    geometry_positions = {}
    for position in range(original_count):
        series = frame.iloc[:, position]
        if position not in geometry_crs and not any(
            isinstance(value, BaseGeometry) for value in series
        ):
            continue
        if any(
            not missing and not isinstance(value, BaseGeometry)
            for value, missing in zip(series, series.isna())
        ):
            raise TypeError("A geometry column contains a non-geometry value")
        values = [
            value if isinstance(value, BaseGeometry) else None for value in series
        ]
        label = "__sedona_native_srid_%d__" % position
        if isinstance(local.columns, pd.MultiIndex):
            label = (label,) + ("",) * (local.columns.nlevels - 1)
        while label in local.columns:
            if isinstance(label, tuple):
                label = (label[0] + "_",) + label[1:]
            else:
                label += "_"
        local.isetitem(
            position,
            pd.Series(
                [
                    shapely.to_wkb(value, flavor="iso") if value is not None else None
                    for value in values
                ],
                index=local.index,
                dtype=object,
            ),
        )
        geometry_positions[position] = len(local.columns)
        crs = geometry_crs.get(position)
        if crs is None:
            srids = [
                int(shapely.get_srid(value)) if value is not None else 0
                for value in values
            ]
        else:
            # Match GeoSeries.set_crs: retain custom WKT metadata with unknown SRID0.
            srids = [CRS.from_user_input(crs).to_epsg() or 0] * len(values)
        local[label] = pd.Series(srids, index=local.index, dtype=np.int32)
    return local, geometry_positions, geometry_crs


def from_pandas(frame, geometry_columns=None):
    """Preserve axes and declared geometry columns at the native Spark boundary.

    ``geometry_columns`` maps known geometry positions to their CRS, including
    None for an explicitly unknown CRS. All-null object columns are otherwise
    ordinary columns; values alone cannot identify their intended type.
    """
    local, geometry_positions, geometry_crs = _prepare_pandas(frame, geometry_columns)
    original_count = len(frame.columns)
    result = ps.DataFrame(local)
    if not geometry_positions:
        return result
    internal = result._internal
    columns = []
    for position in range(original_count):
        column = internal.data_spark_columns[position]
        if position in geometry_positions:
            srid = internal.data_spark_columns[geometry_positions[position]]
            column = ST_SetSRID(ST_GeomFromWKB(column.cast("binary")), srid)
        columns.append(column.alias(internal.data_spark_column_names[position]))
    projected = internal.spark_frame.select(
        *internal.index_spark_columns,
        *columns,
        scol_for(internal.spark_frame, NATURAL_ORDER_COLUMN_NAME),
    )
    names = internal.data_spark_column_names[:original_count]
    fields = [
        (
            InternalField(np.dtype("object"), projected.schema[name])
            if position in geometry_positions
            else internal.data_fields[position]
        )
        for position, name in enumerate(names)
    ]
    fields = [
        (
            with_crs_metadata(field, geometry_crs[position])
            if position in geometry_crs
            else field
        )
        for position, field in enumerate(fields)
    ]
    return ps.DataFrame(
        internal.copy(
            spark_frame=projected,
            data_spark_columns=[scol_for(projected, name) for name in names],
            data_fields=fields,
            column_labels=internal.column_labels[:original_count],
        )
    )


def to_pandas(internal):
    """Collect spatial columns as WKB/SRID structs and return local Shapely values.

    Keeping the Arrow boundary as ordinary structs also avoids Spark's native
    spatial pandas conversion losing integer SRIDs in columns containing nulls.
    """
    columns = []
    geometry_positions = []
    for position, (column, name, field) in enumerate(
        zip(
            internal.data_spark_columns,
            internal.data_spark_column_names,
            internal.data_fields,
        )
    ):
        if isinstance(field.spark_type, GeometryType):
            geometry_positions.append(position)
            column = F.struct(
                ST_AsBinary(column).alias("wkb"), ST_SRID(column).alias("srid")
            )
        columns.append(column.alias(name))
    projected = internal.spark_frame.select(
        *internal.index_spark_columns,
        *columns,
        scol_for(internal.spark_frame, NATURAL_ORDER_COLUMN_NAME),
    )
    fields = [
        field.copy(spark_type=projected.schema[name].dataType)
        for name, field in zip(internal.data_spark_column_names, internal.data_fields)
    ]
    converted = internal.copy(
        spark_frame=projected,
        data_spark_columns=[
            scol_for(projected, name) for name in internal.data_spark_column_names
        ],
        data_fields=fields,
    )
    local = converted.to_pandas_frame
    for position in geometry_positions:

        def decode(value):
            if value is None or value["wkb"] is None:
                return None
            return shapely.set_srid(
                shapely.from_wkb(bytes(value["wkb"])), int(value["srid"])
            )

        local.isetitem(position, local.iloc[:, position].map(decode))
    return local
