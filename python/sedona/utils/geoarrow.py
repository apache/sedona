#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
import itertools
from typing import TYPE_CHECKING, Callable, List

# We may be able to achieve streaming rather than complete materialization by using
# with the ArrowStreamSerializer (instead of the ArrowCollectSerializer)


from sedona.sql.st_functions import ST_AsEWKB
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, DataType, ArrayType, MapType

try:
    from pyspark.util import _load_from_socket
except ImportError:
    from pyspark.rdd import _load_from_socket

from sedona.sql.types import GeometryType
from pyspark.sql.pandas.types import (
    from_arrow_type,
)
from pyspark.sql.pandas.serializers import ArrowStreamPandasSerializer

if TYPE_CHECKING:
    import geopandas as gpd


def dataframe_to_arrow(df, crs=None):
    """
    Collect a DataFrame as a PyArrow Table

    In the output Table, geometry will be encoded as a GeoArrow extension type.
    The resulting output is compatible with `lonboard.viz()`,
    `geopandas.GeoDataFrame.from_arrow()`, or any library compatible with
    GeoArrow extension types.

    :param df: A Spark DataFrame
    :param crs: A CRS-like object (e.g., `pyproj.CRS` or string interpretable by
      `pyproj.CRS`). If provided, this will override any CRS present in the output
      geometries. If omitted, the CRS will be inferred from the values present in
      the output if exactly one CRS is present in the output.
    :return:
    """
    col_is_geometry = [isinstance(f.dataType, GeometryType) for f in df.schema.fields]

    if not any(col_is_geometry):
        return dataframe_to_arrow_raw(df)

    df_projected = project_dataframe_geoarrow(df, col_is_geometry)
    table = dataframe_to_arrow_raw(df_projected)
    return wrap_table_or_batch(table, col_is_geometry, crs)


class GeoArrowDataFrameReader:
    def __init__(self, df, crs=None):
        from pyspark.sql.pandas.types import to_arrow_schema
        from pyspark.traceback_utils import SCCallSiteSync
        from pyspark.sql.pandas.serializers import ArrowCollectSerializer

        self._crs = crs
        self._batch_order = None
        self._col_is_geometry = [
            isinstance(f.dataType, GeometryType) for f in df.schema.fields
        ]

        if any(self._col_is_geometry):
            df = project_dataframe_geoarrow(df, self._col_is_geometry)
            raw_schema = to_arrow_schema(df.schema)
            self._schema = raw_schema_to_geoarrow_schema(
                raw_schema, self._col_is_geometry, self._crs
            )
        else:
            self._schema = to_arrow_schema(df.schema)

        with SCCallSiteSync(df._sc):
            (
                port,
                auth_secret,
                self._jsocket_auth_server,
            ) = df._jdf.collectAsArrowToPython()

        self._batch_stream = _load_from_socket(
            (port, auth_secret), ArrowCollectSerializer()
        )

    @property
    def schema(self):
        return self._schema

    @property
    def batch_order(self):
        return self._batch_order

    def to_table(self):
        import pyarrow as pa

        batches = list(self)
        if not batches:
            return pa.Table.from_batches([], schema=self.schema)

        batches_in_order = [batches[i] for i in self.batch_order]
        return pa.Table.from_batches(batches_in_order)

    def __iter__(self):
        import pyarrow as pa

        try:
            for batch_or_indices in self._batch_stream:
                if isinstance(batch_or_indices, pa.RecordBatch):
                    yield wrap_table_or_batch(
                        batch_or_indices, self._col_is_geometry, self._crs
                    )
                else:
                    self._batch_order = batch_or_indices
        finally:
            self._finish_stream()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self._finish_stream()

    def __del__(self):
        self._finish_stream()

    def _finish_stream(self):
        from pyspark.errors.exceptions.captured import unwrap_spark_exception

        if self._jsocket_auth_server is None:
            return

        with unwrap_spark_exception():
            # Join serving thread and raise any exceptions from collectAsArrowToPython
            auth_server = self._jsocket_auth_server
            self._jsocket_auth_server = None
            auth_server.getResult()


def project_dataframe_geoarrow(df, col_is_geometry):
    df_columns = list(df)
    df_column_names = df.schema.fieldNames()
    for i, is_geom in enumerate(col_is_geometry):
        if is_geom:
            df_columns[i] = ST_AsEWKB(df_columns[i]).alias(df_column_names[i])

    return df.select(*df_columns)


def raw_schema_to_geoarrow_schema(raw_schema, col_is_geometry, crs, columns=None):
    import pyarrow as pa

    try:
        import geoarrow.types as gat

        spec = gat.wkb()
    except ImportError:
        spec = None

    if columns is None:
        columns = [None] * len(col_is_geometry)

    new_fields = [
        (
            wrap_geoarrow_field(raw_schema.field(i), columns[i], crs, spec)
            if is_geom
            else raw_schema.field(i)
        )
        for i, is_geom in enumerate(col_is_geometry)
    ]

    return pa.schema(new_fields)


def wrap_table_or_batch(table_or_batch, col_is_geometry, crs):
    try:
        # Using geoarrow-types is the preferred mechanism for Arrow output.
        # Using the extension type ensures that the type and its metadata will
        # propagate through all pyarrow transformations.
        import geoarrow.types as gat

        try_register_extension_types()

        spec = gat.wkb()

        new_cols = [
            wrap_geoarrow_extension(col, spec, crs) if is_geom else col
            for is_geom, col in zip(col_is_geometry, table_or_batch.columns)
        ]

        return table_or_batch.from_arrays(new_cols, table_or_batch.column_names)
    except ImportError:
        # In the event that we don't have access to GeoArrow extension types,
        # we can still add field metadata that will propagate through some types
        # of operations (e.g., writing this table to a file or passing it to
        # DuckDB as long as no intermediate transformations were applied).
        schema = raw_schema_to_geoarrow_schema(
            table_or_batch.schema, col_is_geometry, crs, table_or_batch.columns
        )
        return table_or_batch.from_arrays(table_or_batch.columns, schema=schema)


def dataframe_to_arrow_raw(df):
    """Backport of toArrow() (available in Spark 4.0)"""
    from pyspark.sql.dataframe import DataFrame

    assert isinstance(df, DataFrame)

    jconf = df.sparkSession._jconf

    from pyspark.sql.pandas.types import to_arrow_schema
    from pyspark.sql.pandas.utils import require_minimum_pyarrow_version

    require_minimum_pyarrow_version()
    schema = to_arrow_schema(df.schema)

    import pyarrow as pa

    self_destruct = jconf.arrowPySparkSelfDestructEnabled()
    batches = df._collect_as_arrow(split_batches=self_destruct)

    # The zero row case can use from_batches() with schema (nothing to cast)
    if not batches:
        return pa.Table.from_batches([], schema)

    # When batches were returned, use cast(schema). This was backported from
    # Spark, where presumably there is a good reason that the schemas of batches
    # may not necessarily align with that of schema (thus a cast is required)
    table = pa.Table.from_batches(batches).cast(schema)

    # Ensure only the table has a reference to the batches, so that
    # self_destruct (if enabled) is effective
    del batches
    return table


def wrap_geoarrow_extension(col, spec, crs):
    if crs is None and col is not None:
        crs = unique_srid_from_ewkb(col)
    elif not hasattr(crs, "to_json"):
        import pyproj

        crs = pyproj.CRS(crs)

    return spec.override(crs=crs).to_pyarrow().wrap_array(col)


def wrap_geoarrow_field(field, col, crs, spec=None):
    if crs is None and col is not None:
        crs = unique_srid_from_ewkb(col)

    if crs is not None:
        metadata = f'"crs": {crs_to_json(crs)}'
    else:
        metadata = ""

    if spec is None:
        return field.with_metadata(
            {
                "ARROW:extension:name": "geoarrow.wkb",
                "ARROW:extension:metadata": "{" + metadata + "}",
            }
        )
    else:
        spec_metadata = spec.from_extension_metadata("{" + metadata + "}")
        return field.with_type(spec_metadata.coalesce(spec).to_pyarrow())


def crs_to_json(crs):
    if hasattr(crs, "to_json"):
        return crs.to_json()
    else:
        import pyproj

        return pyproj.CRS(crs).to_json()


def try_register_extension_types():
    """Try to register extension types using geoarrow-types

    Do this defensively, because it can fail if the extension type was
    registered in some other way (notably: old versions of geoarrow-pyarrow,
    which is a dependency of Kepler).
    """
    from geoarrow.types.type_pyarrow import register_extension_types

    try:
        register_extension_types()
    except RuntimeError:
        pass


def unique_srid_from_ewkb(obj):
    import pyarrow as pa
    import pyarrow.compute as pc

    if len(obj) == 0:
        return None

    # Output shouldn't have mixed endian here
    endian = pc.binary_slice(obj, 0, 1).unique()
    if len(endian) != 1:
        return None

    # WKB Z high byte is 0x80
    # WKB M high byte is is 0x40
    # EWKB SRID high byte is 0x20
    # High bytes where the SRID is set would be
    # [0x20, 0x20 | 0x40, 0x20 | 0x80, 0x20 | 0x40 | 0x80]
    # == [0x20, 0x60, 0xa0, 0xe0]
    is_little_endian = endian[0].as_py() == b"\x01"
    high_byte = (
        pc.binary_slice(obj, 4, 5) if is_little_endian else pc.binary_slice(obj, 1, 2)
    )
    has_srid = pc.is_in(high_byte, pa.array([b"\x20", b"\x60", b"\xa0", b"\xe0"]))
    unique_srids = (
        pc.if_else(has_srid, pc.binary_slice(obj, 5, 9), None).unique().drop_null()
    )
    if len(unique_srids) != 1:
        return None

    srid_bytes = unique_srids[0].as_py()
    endian = "little" if is_little_endian else "big"
    epsg_code = int.from_bytes(srid_bytes, endian)

    import pyproj

    return pyproj.CRS(f"EPSG:{epsg_code}")


def _dedup_names(names: List[str]) -> List[str]:
    if len(set(names)) == len(names):
        return names
    else:

        def _gen_dedup(_name: str) -> Callable[[], str]:
            _i = itertools.count()
            return lambda: f"{_name}_{next(_i)}"

        def _gen_identity(_name: str) -> Callable[[], str]:
            return lambda: _name

        gen_new_name = {
            name: _gen_dedup(name) if len(list(group)) > 1 else _gen_identity(name)
            for name, group in itertools.groupby(sorted(names))
        }
        return [gen_new_name[name]() for name in names]


# Backport from Spark 4.0
# https://github.com/apache/spark/blob/3515b207c41d78194d11933cd04bddc21f8418dd/python/pyspark/sql/pandas/types.py#L1385
def _deduplicate_field_names(dt: DataType) -> DataType:
    if isinstance(dt, StructType):
        dedup_field_names = _dedup_names(dt.names)

        return StructType(
            [
                StructField(
                    dedup_field_names[i],
                    _deduplicate_field_names(field.dataType),
                    nullable=field.nullable,
                )
                for i, field in enumerate(dt.fields)
            ]
        )
    elif isinstance(dt, ArrayType):
        return ArrayType(
            _deduplicate_field_names(dt.elementType), containsNull=dt.containsNull
        )
    elif isinstance(dt, MapType):
        return MapType(
            _deduplicate_field_names(dt.keyType),
            _deduplicate_field_names(dt.valueType),
            valueContainsNull=dt.valueContainsNull,
        )
    else:
        return dt


def infer_schema(gdf: "gpd.GeoDataFrame") -> StructType:
    import pyarrow as pa

    fields = gdf.dtypes.reset_index().values.tolist()
    geom_fields = []
    index = 0
    for name, dtype in fields:
        if dtype == "geometry":
            geom_fields.append((index, name))
            continue

        index += 1

    if not geom_fields:
        raise ValueError("No geometry field found in the GeoDataFrame")

    pa_schema = pa.Schema.from_pandas(
        gdf.drop([name for _, name in geom_fields], axis=1)
    )

    spark_schema = []

    for field in pa_schema:
        field_type = field.type
        spark_type = from_arrow_type(field_type)
        spark_schema.append(StructField(field.name, spark_type, True))

    for index, geom_field in geom_fields:
        spark_schema.insert(index, StructField(geom_field, GeometryType(), True))

    return StructType(spark_schema)


# Modified backport from Spark 4.0
# https://github.com/apache/spark/blob/3515b207c41d78194d11933cd04bddc21f8418dd/python/pyspark/sql/pandas/conversion.py#L632
def create_spatial_dataframe(spark: SparkSession, gdf: "gpd.GeoDataFrame") -> DataFrame:
    from pyspark.sql.pandas.types import (
        to_arrow_type,
    )

    def reader_func(temp_filename):
        return spark._jvm.PythonSQLUtils.readArrowStreamFromFile(temp_filename)

    def create_iter_server():
        return spark._jvm.ArrowIteratorServer()

    schema = infer_schema(gdf)
    timezone = spark._jconf.sessionLocalTimeZone()
    step = spark._jconf.arrowMaxRecordsPerBatch()
    step = step if step > 0 else len(gdf)
    pdf_slices = (gdf.iloc[start : start + step] for start in range(0, len(gdf), step))
    spark_types = [_deduplicate_field_names(f.dataType) for f in schema.fields]

    arrow_data = [
        [
            (c, to_arrow_type(t) if t is not None else None, t)
            for (_, c), t in zip(pdf_slice.items(), spark_types)
        ]
        for pdf_slice in pdf_slices
    ]

    safecheck = spark._jconf.arrowSafeTypeConversion()
    ser = ArrowStreamPandasSerializer(timezone, safecheck)
    jiter = spark._sc._serialize_to_jvm(
        arrow_data, ser, reader_func, create_iter_server
    )

    jsparkSession = spark._jsparkSession
    jdf = spark._jvm.PythonSQLUtils.toDataFrame(jiter, schema.json(), jsparkSession)

    df = DataFrame(jdf, spark)

    df._schema = schema

    return df
