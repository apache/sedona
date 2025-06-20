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
import os
import shutil
import tempfile
from geopandas.testing import assert_geoseries_equal

from shapely.geometry import (
    Point,
    Polygon,
)

from sedona.geopandas import GeoSeries
from tests.test_base import TestBase
import pyspark.pandas as ps


class TestSeries(TestBase):
    def setup_method(self):
        self.tempdir = tempfile.mkdtemp()
        self.t1 = Polygon([(0, 0), (1, 0), (1, 1)])
        self.t2 = Polygon([(0, 0), (1, 1), (0, 1)])
        self.sq = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        self.g1 = GeoSeries([self.t1, self.t2])
        self.g2 = GeoSeries([self.sq, self.t1])
        self.g3 = GeoSeries([self.t1, self.t2], crs="epsg:4326")
        self.g4 = GeoSeries([self.t2, self.t1])

    def teardown_method(self):
        shutil.rmtree(self.tempdir)

    def test_constructor(self):
        s = GeoSeries([Point(x, x) for x in range(3)])
        check_geoseries_equal(s, s)

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
        assert psdf.count().count() == 3

    def test_internal_st_function(self):
        # this is to make sure the spark session works with internal sedona udfs
        baseDf = self.spark.sql(
            "SELECT ST_GeomFromWKT('POLYGON ((50 50 1, 50 80 2, 80 80 3, 80 50 2, 50 50 1))') as geom"
        )
        actual = baseDf.selectExpr("ST_AsText(ST_Expand(geom, 10))").first()[0]
        expected = "POLYGON Z((40 40 -9, 40 90 -9, 90 90 13, 90 40 13, 40 40 -9))"
        assert expected == actual

    def test_type(self):
        assert type(self.g1) is GeoSeries
        assert type(self.g2) is GeoSeries
        assert type(self.g3) is GeoSeries
        assert type(self.g4) is GeoSeries

    def test_copy(self):
        gc = self.g3.copy()
        assert type(gc) is GeoSeries
        assert self.g3.name == gc.name

    def test_area(self):
        area = self.g1.area
        assert area is not None
        assert type(area) is GeoSeries
        assert area.count() == 2

    def test_buffer(self):
        buffer = self.g1.buffer(0.2)
        assert buffer is not None
        assert type(buffer) is GeoSeries
        assert buffer.count() == 2

    def test_buffer_then_area(self):
        area = self.g1.buffer(0.2).area
        assert area is not None
        assert type(area) is GeoSeries
        assert area.count() == 2

    def test_buffer_then_geoparquet(self):
        temp_file_path = os.path.join(
            self.tempdir, next(tempfile._get_candidate_names()) + ".parquet"
        )
        self.g1.buffer(0.2).to_parquet(temp_file_path)
        assert os.path.exists(temp_file_path)


# -----------------------------------------------------------------------------
# # Utils
# -----------------------------------------------------------------------------


def check_geoseries_equal(s1, s2):
    assert isinstance(s1, GeoSeries)
    assert isinstance(s1.geometry, GeoSeries)
    assert isinstance(s2, GeoSeries)
    assert isinstance(s2.geometry, GeoSeries)
    if isinstance(s1, GeoSeries):
        s1 = s1.to_geopandas()
    if isinstance(s2, GeoSeries):
        s2 = s2.to_geopandas()
    assert_geoseries_equal(s1, s2)
