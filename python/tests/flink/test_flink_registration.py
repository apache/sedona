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

from shapely.wkb import loads
import pytest

from tests.flink.conftest import has_pyflink

if not has_pyflink():
    pytest.skip(
        "PyFlink is not installed. Skipping tests that require PyFlink.",
        allow_module_level=True,
    )


def test_register(table_env):
    result = (
        table_env.sql_query("SELECT ST_ASBinary(ST_Point(1.0, 2.0))")
        .execute()
        .collect()
    )

    assert 1 == len([el for el in result])


def test_register_udf(table_env):
    from pyflink.table.udf import ScalarFunction, udf

    class Buffer(ScalarFunction):
        def eval(self, s):
            geom = loads(s)
            return geom.buffer(1).wkb

    table_env.create_temporary_function(
        "ST_BufferPython", udf(Buffer(), result_type="Binary")
    )

    buffer_table = table_env.sql_query(
        "SELECT ST_BufferPython(ST_ASBinary(ST_Point(1.0, 2.0))) AS buffer"
    )

    table_env.create_temporary_view("buffer_table", buffer_table)

    result = (
        table_env.sql_query("SELECT ST_Area(ST_GeomFromWKB(buffer)) FROM buffer_table")
        .execute()
        .collect()
    )

    items = [el for el in result]
    area = items[0][0]

    assert 3.12 < area < 3.14
    assert 1 == len(items)
