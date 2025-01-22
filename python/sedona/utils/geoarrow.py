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

# We may be able to acheive streaming rather than complete materialization by using
# with the ArrowStreamSerializer (instead of the ArrowCollectSerializer)

import pyarrow as pa
from sedona.sql.types import GeometryType


def dataframe_to_arrow(df):
    spark_schema = df.schema
    table = dataframe_to_arrow_raw(df, spark_schema)

    col_is_geometry = [
        isinstance(f.dataType, GeometryType) for f in spark_schema.fields
    ]
    new_cols = [
        to_geoarrow(col) if is_geom else col
        for is_geom, col in zip(col_is_geometry, table.columns)
    ]

    return pa.table(new_cols, table.column_names)


def to_geoarrow(col):
    # TODO: actually convert
    return col


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
