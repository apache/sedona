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
import geoarrow.types as gat
from sedona.sql.types import GeometryType


def dataframe_to_arrow(df):
    spark_schema = df.schema
    table = dataframe_to_arrow_raw(df, spark_schema)

    col_is_geometry = [
        isinstance(f.dataType, GeometryType) for f in spark_schema.fields
    ]

    target_type = gat.wkb().to_pyarrow()
    new_cols = [
        to_geoarrow(col, target_type) if is_geom else col
        for is_geom, col in zip(col_is_geometry, table.columns)
    ]

    return pa.table(new_cols, table.column_names)


def to_geoarrow(col, target_type):
    # We could in theory support other string layouts here, but Spark always sends
    # binary() so this should be a noop unless Spark changes something.
    col.cast(pa.binary())

    chunks = [to_geoarrow_chunk(chunk, target_type) for chunk in col.chunks]
    return pa.chunked_array(chunks, target_type)


def to_geoarrow_chunk(chunk, target_type):
    # Validate the assumption that this isn't a sliced array
    assert chunk.offset == 0

    length = len(chunk)

    # Get the raw input buffers
    validity, offsets_in, data_in = chunk.buffers()

    offsets_out, data_out = to_geoarrow_chunk_buffers(
        length, validity, offsets_in, data_in, offsets_in.size, data_in.size
    )
    storage = pa.Array.from_buffers(
        target_type.storage_type,
        length,
        (validity, pa.py_buffer(offsets_out), pa.py_buffer(data_out)),
        chunk.null_count,
    )
    return target_type.wrap_array(storage)


def to_geoarrow_chunk_buffers(
    length, validity_in, offsets_in, data_in, offsets_out_size, data_out_size
):
    # To simplify the C code required, we'll allocate the buffers in Python.
    # The offsets buffer will be the same size (binary in -> binary_out);
    # the data buffer will *usually* be long enough except if the input contains
    # POINT EMPTY.
    offsets_out = bytearray(offsets_out_size)
    data_out = bytearray(data_out_size)

    for _ in range(5):
        success = to_geoarrow_chunk_buffers_impl(
            length, validity_in, offsets_in, data_in, offsets_out, data_out
        )
        if success:
            return offsets_out, data_out

        # Allocate more and try again
        data_out_size = int(data_out_size * 2)
        data_out = bytearray(data_out_size)

    raise ValueError("Failed to allocate enough memory for output data buffer")


def to_geoarrow_chunk_buffers_impl(
    length, validity_in, offsets_in, data_in, offsets_out, data_out
):
    # TODO: this part will be in C
    print(length, validity_in, offsets_in, data_in, offsets_out, data_out)
    return True


def dataframe_to_arrow_raw(df, spark_schema):
    """Backport of toArrow() (available in Spark 4.0)"""
    from pyspark.sql.dataframe import DataFrame

    assert isinstance(df, DataFrame)

    jconf = df.sparkSession._jconf

    from pyspark.sql.pandas.types import to_arrow_schema
    from pyspark.sql.pandas.utils import require_minimum_pyarrow_version

    require_minimum_pyarrow_version()
    schema = to_arrow_schema(spark_schema)

    import pyarrow as pa

    self_destruct = jconf.arrowPySparkSelfDestructEnabled()
    batches = df._collect_as_arrow(split_batches=self_destruct)
    table = pa.Table.from_batches(batches).cast(schema)
    # Ensure only the table has a reference to the batches, so that
    # self_destruct (if enabled) is effective
    del batches
    return table
