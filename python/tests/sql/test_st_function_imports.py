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

"""module without top level imports of these names"""

import importlib

import pytest

from tests.test_base import TestBase

DATAFRAME_API_MODULES = [
    "sedona.spark.sql.st_aggregates",
    "sedona.spark.sql.st_constructors",
    "sedona.spark.sql.st_functions",
    "sedona.spark.sql.st_predicates",
]
DATAFRAME_API_HELPERS = {"call_sedona_function", "lit", "validate_argument_types"}


class TestStFunctionImport(TestBase):
    def test_import(self):
        from sedona.spark.sql import (
            ST_Contains,
            ST_Distance,
            ST_Envelope_Aggr,
            ST_Point,
        )

        ST_Distance
        ST_Point
        ST_Contains
        ST_Envelope_Aggr

    def test_geometry_type_should_be_a_sql_type(self):
        from pyspark.sql.types import UserDefinedType

        from sedona.spark import GeometryType

        assert isinstance(GeometryType(), UserDefinedType)


@pytest.mark.parametrize("module_name", DATAFRAME_API_MODULES)
def test_all_lists_only_the_functions_defined_in_the_module(module_name):
    module = importlib.import_module(module_name)
    assert module.__all__
    assert DATAFRAME_API_HELPERS.isdisjoint(module.__all__)
    for name in module.__all__:
        assert getattr(module, name).__module__ == module_name, name


def test_star_import_does_not_export_dataframe_api_helpers():
    namespace = {}
    exec("from sedona.spark import *", namespace)
    assert "ST_Point" in namespace
    assert DATAFRAME_API_HELPERS.isdisjoint(namespace)
