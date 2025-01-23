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
from tests.test_base import TestBase
from pyspark.sql.types import StringType, StructType

from sedona.utils.geoarrow import dataframe_to_arrow


class TestGeoArrowSerde(TestBase):
    def test_to_geoarrow_no_geometry(self):
        schema = StructType().add("wkt", StringType())
        wkt_df = TestGeoArrowSerde.spark.createDataFrame(zip(TEST_WKT), schema)
        wkt_table = dataframe_to_arrow(wkt_df)
        assert wkt_table == pa.table({"wkt": TEST_WKT})

    def test_to_geoarrow_with_geometry(self):
        schema = StructType().add("wkt", StringType())
        wkt_df = TestGeoArrowSerde.spark.createDataFrame(zip(TEST_WKT), schema)
        geo_df = wkt_df.selectExpr("wkt", "ST_GeomFromText(wkt) AS geom")

        geo_table = dataframe_to_arrow(geo_df)
        assert geo_table.column_names == ["wkt", "geom"]

        geom = geo_table["geom"]
        if isinstance(geom.type, pa.ExtensionType):
            assert geom.type.extension_name == "geoarrow.wkb"
        else:
            field = geo_table.field("geom")
            assert field.metadata is not None
            assert b"ARROW:extension:name" in field.metadata
            assert field.metadata[b"ARROW:extension:name"] == b"geoarrow.wkb"


TEST_WKT = [
    "POINT (10 20)",
    "POINT (10 20 30)",
    "LINESTRING (10 20, 30 40)",
    "LINESTRING (10 20 30, 40 50 60)",
    "POLYGON ((10 10, 20 20, 20 10, 10 10))",
    "POLYGON ((10 10 10, 20 20 10, 20 10 10, 10 10 10))",
    "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))",
]
