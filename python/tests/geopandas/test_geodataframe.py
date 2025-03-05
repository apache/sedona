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
import os
import shutil
import tempfile

from numpy.dtypes import ObjectDType
from shapely.geometry import (
    Point,
)

from sedona.geopandas import GeoDataFrame
from tests.test_base import TestBase
import pyspark.pandas as ps


class TestDataframe(TestBase):
    def setup_method(self):
        N = 10
        self.tempdir = tempfile.mkdtemp()
        self.crs = "epsg:4326"
        self.df = GeoDataFrame(
            [
                {"geometry": Point(x, y), "value1": x + y, "value2": x * y}
                for x, y in zip(range(N), range(N))
            ],
            crs=self.crs,
        )

    def teardown_method(self):
        shutil.rmtree(self.tempdir)

    def test_constructor(self):
        s = GeoDataFrame([Point(x, x) for x in range(3)])
        check_geoseries(s)

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
        assert type(self.g1) is GeoDataFrame

    def test_copy(self):
        gc = self.g3.copy()
        assert type(gc) is GeoDataFrame
        assert self.g3.name == gc.name

    def test_area(self):
        area = self.g1.area
        assert area is not None
        assert type(area) is GeoDataFrame
        assert area.count() is 2

    def test_buffer(self):
        buffer = self.g1.buffer(0.2)
        assert buffer is not None
        assert type(buffer) is GeoDataFrame
        assert buffer.count() is 2

    def test_buffer_then_area(self):
        area = self.g1.buffer(0.2).area
        assert area is not None
        assert type(area) is GeoDataFrame
        assert area.count() is 2

    def test_buffer_then_geoparquet(self):
        temp_file_path = os.path.join(
            self.tempdir, next(tempfile._get_candidate_names()) + ".parquet"
        )
        self.g1.buffer(0.2).to_parquet(temp_file_path)
        assert os.path.exists(temp_file_path)


# -----------------------------------------------------------------------------
# # Utils
# -----------------------------------------------------------------------------


def check_geoseries(s):
    assert isinstance(s, GeoDataFrame)
    assert isinstance(s.geometry, GeoDataFrame)
    assert isinstance(s.dtype, ObjectDType)
