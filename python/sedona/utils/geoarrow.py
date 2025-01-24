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

# We may be able to achieve streaming rather than complete materialization by using
# with the ArrowStreamSerializer (instead of the ArrowCollectSerializer)

import pyarrow as pa
import pyarrow.compute as pc

from sedona.sql.types import GeometryType
from sedona.sql.st_functions import ST_AsEWKB


def dataframe_to_arrow(df, crs=None):
    col_is_geometry = [isinstance(f.dataType, GeometryType) for f in df.schema.fields]

    if not any(col_is_geometry):
        return dataframe_to_arrow_raw(df)

    df_columns = list(df)
    df_column_names = df.schema.fieldNames()
    for i, is_geom in enumerate(col_is_geometry):
        if is_geom:
            df_columns[i] = ST_AsEWKB(df_columns[i]).alias(df_column_names[i])

    df_projected = df.select(*df_columns)
    table = dataframe_to_arrow_raw(df_projected)

    try:
        # Using geoarrow-types is the preferred mechanism for Arrow output.
        # Using the extension type ensures that the type and its metadata will
        # propagate through all pyarrow transformations.
        import geoarrow.types as gat
        from geoarrow.types.type_pyarrow import register_extension_types

        register_extension_types()
        target_type = gat.wkb().to_pyarrow()

        new_cols = [
            wrap_geoarrow_extension(col, target_type, crs) if is_geom else col
            for is_geom, col in zip(col_is_geometry, table.columns)
        ]

        return pa.table(new_cols, table.column_names)
    except ImportError:
        # In the event that we don't have access to GeoArrow extension types,
        # we can still add field metadata that will propagate through some types
        # of operations (e.g., writing this table to a file or passing it to
        # DuckDB as long as no intermediate transformations were applied).
        new_fields = [
            (
                wrap_geoarrow_field(table.schema.field(i), table[i], crs)
                if is_geom
                else table.schema.field(i)
            )
            for i, is_geom in enumerate(col_is_geometry)
        ]

        return table.from_arrays(table.columns, schema=pa.schema(new_fields))


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
    table = pa.Table.from_batches(batches).cast(schema)
    # Ensure only the table has a reference to the batches, so that
    # self_destruct (if enabled) is effective
    del batches
    return table


def wrap_geoarrow_extension(col, wkb_type, crs):
    if crs is None:
        crs = unique_srid_from_ewkb(col)

    return wkb_type.with_crs(crs).wrap_array(col)


def wrap_geoarrow_field(field, col, crs):
    if crs is None:
        crs = unique_srid_from_ewkb(col)

    if crs is not None:
        metadata = f'"crs": {crs_to_json(crs)}'
    else:
        metadata = ""

    return field.with_metadata(
        {
            "ARROW:extension:name": "geoarrow.wkb",
            "ARROW:extension:metadata": "{" + metadata + "}",
        }
    )


def crs_to_json(crs):
    if hasattr(crs, "to_json"):
        return crs.to_json()
    else:
        import pyproj

        return pyproj.CRS(crs).to_json()


def unique_srid_from_ewkb(obj):
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
    unique_srids = pc.if_else(has_srid, pc.binary_slice(obj, 5, 9), None).unique()
    if len(unique_srids) != 1:
        return None

    srid_bytes = unique_srids[0].as_py()
    endian = "little" if is_little_endian else "big"
    epsg_code = int.from_bytes(srid_bytes, endian)

    import pyproj

    return pyproj.CRS(f"EPSG:{epsg_code}")
