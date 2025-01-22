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

import pyarrow as pa
from pyspark.sql.functions import expr
from tests.test_base import TestBase
from pyspark.sql.types import StringType, StructType

from sedona.utils.geoarrow import dataframe_to_arrow


class TestGeometrySerde(TestBase):
    def test_to_geoarrow_no_geometry(self):
        schema = StructType().add("wkt", StringType())
        wkt_df = TestGeometrySerde.spark.createDataFrame([zip(TEST_WKT)], schema)
        wkt_table = dataframe_to_arrow(wkt_df)
        assert wkt_table == pa.table({"wkt": TEST_WKT})


TEST_WKT = [
    # empty geometries
    "POINT EMPTY",
    "LINESTRING EMPTY",
    "POLYGON EMPTY",
    "MULTIPOINT EMPTY",
    "MULTILINESTRING EMPTY",
    "MULTIPOLYGON EMPTY",
    "GEOMETRYCOLLECTION EMPTY",
    # non-empty geometries
    "POINT (10 20)",
    "POINT (10 20 30)",
    "LINESTRING (10 20, 30 40)",
    "LINESTRING (10 20 30, 40 50 60)",
    "POLYGON ((10 10, 20 20, 20 10, 10 10))",
    "POLYGON ((10 10 10, 20 20 10, 20 10 10, 10 10 10))",
    "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))",
    # non-empty multi geometries
    "MULTIPOINT ((10 20), (30 40))",
    "MULTIPOINT ((10 20 30), (40 50 60))",
    "MULTILINESTRING ((10 20, 30 40), (50 60, 70 80))",
    "MULTILINESTRING ((10 20 30, 40 50 60), (70 80 90, 100 110 120))",
    "MULTIPOLYGON (((10 10, 20 20, 20 10, 10 10)), ((-10 -10, -20 -20, -20 -10, -10 -10)))",
    "MULTIPOLYGON (((10 10, 20 20, 20 10, 10 10)), ((0 0, 0 10, 10 10, 10 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1)))",
    "GEOMETRYCOLLECTION (POINT (10 20), LINESTRING (10 20, 30 40))",
    "GEOMETRYCOLLECTION (POINT (10 20 30), LINESTRING (10 20 30, 40 50 60))",
    "GEOMETRYCOLLECTION (POINT (10 20), LINESTRING (10 20, 30 40), POLYGON ((10 10, 20 20, 20 10, 10 10)))",
    # nested geometry collection
    "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (10 20), LINESTRING (10 20, 30 40)))",
    "GEOMETRYCOLLECTION (POINT (1 2), GEOMETRYCOLLECTION (POINT (10 20), LINESTRING (10 20, 30 40)))",
    # multi geometries containing empty geometries
    "MULTIPOINT (EMPTY, (10 20))",
    "MULTIPOINT (EMPTY, EMPTY)",
    "MULTILINESTRING (EMPTY, (10 20, 30 40))",
    "MULTILINESTRING (EMPTY, EMPTY)",
    "MULTIPOLYGON (EMPTY, ((10 10, 20 20, 20 10, 10 10)))",
    "MULTIPOLYGON (EMPTY, EMPTY)",
    "GEOMETRYCOLLECTION (POINT (10 20), POINT EMPTY, LINESTRING (10 20, 30 40))",
    "GEOMETRYCOLLECTION (MULTIPOINT EMPTY, MULTILINESTRING EMPTY, MULTIPOLYGON EMPTY, GEOMETRYCOLLECTION EMPTY)",
]
