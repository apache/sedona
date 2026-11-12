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
from pyspark.pandas.internal import (
    InternalField,
    InternalFrame,
    NATURAL_ORDER_COLUMN_NAME,
)
from pyspark.pandas.namespace import _get_index_map
from pyspark.sql.types import ArrayType
from pyspark.pandas.utils import scol_for

from sedona.spark.sql.st_constructors import ST_GeomFromWKB
from sedona.spark.sql.st_functions import ST_SetSRID, ST_AsBinary, ST_SRID
from sedona.spark.sql.types import GeometryType
from sedona.spark.geopandas._crs import with_crs_metadata
from pyspark.sql import functions as F


def internal_field(field):
    """Describe spatial values explicitly instead of asking Arrow to infer them."""
    data_type = field.dataType
    while isinstance(data_type, ArrayType):
        data_type = data_type.elementType
    if isinstance(data_type, GeometryType):
        return InternalField(np.dtype("object"), field)
    return InternalField.from_struct_field(field)


def from_spark(frame, index_col=None):
    """Wrap a Spark frame without inferring native geometry through Arrow."""
    indexes, index_names = _get_index_map(frame, index_col)
    index_columns = (
        []
        if index_col is None
        else ([index_col] if isinstance(index_col, str) else list(index_col))
    )
    names = [
        name
        for name in frame.columns
        if name not in index_columns and name != NATURAL_ORDER_COLUMN_NAME
    ]
    return ps.DataFrame(
        InternalFrame(
            spark_frame=frame,
            index_spark_columns=indexes,
            index_names=index_names,
            data_spark_columns=[scol_for(frame, name) for name in names],
            data_fields=[internal_field(frame.schema[name]) for name in names],
        )
    )


class NativeScalarSeries(ps.Series):
    """Collect a scalar column without reading native geometry in its anchor."""

    def _to_internal_pandas(self):
        # Keep the shared anchor during distributed operations. Project only
        # when collecting, since pandas-on-Spark cannot restore native geometry.
        return to_pandas(self._internal).iloc[:, 0]

    def __repr__(self):
        return display(self)


class _NativeDisplayFrame(ps.DataFrame):
    def _to_pandas(self):
        return self._to_internal_pandas().copy()

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
            geometry_positions.append((position, False))
            column = _geometry_struct(column)
        elif isinstance(field.spark_type, ArrayType) and isinstance(
            field.spark_type.elementType, GeometryType
        ):
            geometry_positions.append((position, True))
            column = F.when(
                column.isNotNull(),
                F.struct(
                    F.transform(column, ST_AsBinary).alias("wkb"),
                    F.transform(column, ST_SRID).alias("srid"),
                ),
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
    for position, is_array in geometry_positions:

        def decode(value):
            if value is None or value["wkb"] is None:
                return None
            return shapely.set_srid(
                shapely.from_wkb(bytes(value["wkb"])), int(value["srid"])
            )

        if is_array:
            local.isetitem(
                position,
                local.iloc[:, position].map(
                    lambda values: (
                        None
                        if values is None
                        else [
                            decode({"wkb": wkb, "srid": srid})
                            for wkb, srid in zip(values["wkb"], values["srid"])
                        ]
                    )
                ),
            )
        else:
            local.isetitem(position, local.iloc[:, position].map(decode))
    return local


def fixed_srid(column, srid):
    """Declare a fixed native CRS only after validating every existing SRID."""
    checked = F.when(
        column.isNull() | (ST_SRID(column) == F.lit(srid)), column
    ).otherwise(
        F.raise_error(
            F.lit("Native geometry SRID does not match the declared CRS " + str(srid))
        )
    )
    # Spark does not cast ANY geometry to a fixed CRS. This identity operation
    # becomes fixed-typed, but its guard prevents any change to a stored SRID.
    return ST_SetSRID(checked, srid)


def _geometry_struct(column):
    return F.struct(ST_AsBinary(column).alias("wkb"), ST_SRID(column).alias("srid"))


def _encode_geometry_columns(internal):
    columns = [
        (
            _geometry_struct(column)
            if isinstance(field.spark_type, GeometryType)
            else column
        ).alias(name, metadata=field.metadata)
        for column, name, field in zip(
            internal.data_spark_columns,
            internal.data_spark_column_names,
            internal.data_fields,
        )
    ]
    projected = internal.spark_frame.select(
        *internal.index_spark_columns,
        *columns,
        scol_for(internal.spark_frame, NATURAL_ORDER_COLUMN_NAME),
    )
    return internal.copy(
        spark_frame=projected,
        data_spark_columns=[
            scol_for(projected, name) for name in internal.data_spark_column_names
        ],
        data_fields=[
            field.copy(spark_type=projected.schema[name].dataType)
            for name, field in zip(
                internal.data_spark_column_names, internal.data_fields
            )
        ],
    )


def assign_geometry(frame, key, series):
    """Align geometry with ordinary WKB/SRID structs, then restore native values."""
    from pyspark.pandas.series import first_series

    fields = dict(zip(frame._internal.column_labels, frame._internal.data_fields))
    label = ps.DataFrame._index_normalized_label(
        frame._internal.column_labels_level, key
    )[0]
    fields[label] = series._internal.data_fields[0]
    aligned = ps.DataFrame(_encode_geometry_columns(frame._internal))
    aligned[key] = first_series(
        ps.DataFrame(_encode_geometry_columns(series._internal))
    )
    return _restore_geometry_columns(aligned._internal, fields)


def _restore_geometry_columns(internal, fields):
    columns = []
    restored_fields = []
    for label, name, column, field in zip(
        internal.column_labels,
        internal.data_spark_column_names,
        internal.data_spark_columns,
        internal.data_fields,
    ):
        original = fields.get(label)
        if original is not None and isinstance(original.spark_type, GeometryType):
            column = ST_SetSRID(ST_GeomFromWKB(column["wkb"]), column["srid"])
            if original.spark_type.srid != GeometryType.MIXED_SRID:
                column = fixed_srid(column, original.spark_type.srid)
            field = original.copy(name=name, nullable=True)
        columns.append(column.alias(name, metadata=field.metadata))
        restored_fields.append(field)
    return internal.with_new_columns(columns, data_fields=restored_fields)


class _NativeIndexCollection:
    def equals(self, other):
        if not isinstance(other, ps.Index):
            return False

        def plain(index):
            internal = index._psdf._internal.copy(
                column_labels=[], data_spark_columns=[], data_fields=[]
            )
            return ps.DataFrame(internal).index

        return plain(self).equals(plain(other))

    def _to_internal_pandas(self):
        internal = self._psdf._internal.copy(
            column_labels=[],
            data_spark_columns=[],
            data_fields=[],
        )
        return internal.to_pandas_frame.index


class _NativeIndex(_NativeIndexCollection, ps.Index):
    pass


class _NativeMultiIndex(_NativeIndexCollection, ps.MultiIndex):
    pass


class _NativeDatetimeIndex(_NativeIndexCollection, ps.DatetimeIndex):
    pass


class _NativeTimedeltaIndex(_NativeIndexCollection, ps.TimedeltaIndex):
    pass


class _NativeCategoricalIndex(_NativeIndexCollection, ps.CategoricalIndex):
    pass


def native_index(index):
    index_type = {
        ps.Index: _NativeIndex,
        ps.MultiIndex: _NativeMultiIndex,
        ps.DatetimeIndex: _NativeDatetimeIndex,
        ps.TimedeltaIndex: _NativeTimedeltaIndex,
        ps.CategoricalIndex: _NativeCategoricalIndex,
    }.get(type(index))
    if index_type is None:
        return index
    result = object.__new__(index_type)
    result._anchor = index._anchor
    return result


class NativeGeometryILoc:
    """Keep positional selection distributed and decode collected scalar values."""

    def __init__(self, series):
        self._series = series

    def __getitem__(self, key):
        from pyspark.pandas.series import first_series
        from sedona.spark.geopandas.geoseries import GeoSeries

        if isinstance(key, (int, np.integer)):
            encoded = first_series(
                ps.DataFrame(_encode_geometry_columns(self._series._internal))
            )
            value = encoded.iloc[key]
            if value is None or value["wkb"] is None:
                return None
            return shapely.set_srid(
                shapely.from_wkb(bytes(value["wkb"])), int(value["srid"])
            )
        selected = ps.Series.iloc.fget(self._series)[key]
        return GeoSeries(selected) if isinstance(selected, ps.Series) else selected

    def __setitem__(self, key, value):
        from sedona.spark.geopandas.geoseries import _geometry_literal

        original = self._series._anchor._internal
        fields = dict(zip(original.column_labels, original.data_fields))
        encoded = ps.DataFrame(_encode_geometry_columns(original))
        encoded_series = encoded._psser_for(self._series._column_label)
        if isinstance(value, BaseGeometry):
            value = _geometry_struct(_geometry_literal(value))
            if isinstance(key, (int, np.integer)):
                # pandas-on-Spark rejects Column values for a scalar indexer.
                # A one-position selection retains the same row assignment.
                key = [int(key)]
        elif isinstance(value, ps.Series) and isinstance(
            value.spark.data_type, GeometryType
        ):
            value = ps.DataFrame(_encode_geometry_columns(value._internal))._psser_for(
                value._column_label
            )
        encoded_series.iloc[key] = value
        restored = _restore_geometry_columns(encoded_series._anchor._internal, fields)
        self._series._update_internal_frame(restored, check_same_anchor=False)
        self._series._sindex = None
