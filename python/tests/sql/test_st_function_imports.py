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

"""module without top level imports of these names"""

from tests.test_base import TestBase


class TestStFunctionImport(TestBase):
    def test_import(self):
        from sedona.sql import (
            ST_Distance,
            ST_Point,
            ST_Contains,
            ST_Envelope_Aggr,
        )

        ST_Distance
        ST_Point
        ST_Contains
        ST_Envelope_Aggr

    def test_geometry_type_should_be_a_sql_type(self):
        from sedona.spark import GeometryType
        from pyspark.sql.types import UserDefinedType
        assert isinstance(GeometryType(), UserDefinedType)
