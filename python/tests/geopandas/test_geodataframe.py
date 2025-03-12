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
import shutil
import tempfile

from shapely.geometry import (
    Point,
)

from sedona.geopandas import GeoDataFrame
from tests.test_base import TestBase
import pyspark.pandas as ps


class TestDataframe(TestBase):
    # def setup_method(self):
    #     N = 10
    #     self.tempdir = tempfile.mkdtemp()
    #     self.crs = "epsg:4326"
    #     self.df = GeoDataFrame(
    #         [
    #             {"geometry": Point(x, y), "value1": x + y, "value2": x * y}
    #             for x, y in zip(range(N), range(N))
    #         ],
    #         crs=self.crs,
    #     )
    #
    # def teardown_method(self):
    #     shutil.rmtree(self.tempdir)

    def test_constructor(self):
        df = GeoDataFrame([Point(x, x) for x in range(3)])
        check_geodataframe(df)

    def test_psdf(self):
        # this is to make sure the spark session works with pandas on spark api
        psdf = ps.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6],
                "b": [100, 200, 300, 400, 500, 600],
                "c": ["one", "two", "three", "four", "five", "six"],
            },
            index=[10, 20, 30, 40, 50, 60],
        )
        assert psdf.count().count() is 3

    def test_type(self):
        df = GeoDataFrame([Point(x, x) for x in range(3)])
        assert type(df) is GeoDataFrame

    def test_copy(self):
        df = GeoDataFrame([Point(x, x) for x in range(3)], name="test_df")
        df_copy = df.copy()
        assert type(df_copy) is GeoDataFrame


# -----------------------------------------------------------------------------
# # Utils
# -----------------------------------------------------------------------------


def check_geodataframe(df):
    assert isinstance(df, GeoDataFrame)
