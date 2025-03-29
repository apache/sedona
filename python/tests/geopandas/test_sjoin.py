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

from shapely.geometry import Polygon
from sedona.geopandas import GeoSeries, sjoin
from tests.test_base import TestBase


class TestSpatialJoin(TestBase):
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

    def test_sjoin_method1(self):
        left = self.g1
        right = self.g2
        joined = sjoin(left, right)
        assert joined is not None
        assert type(joined) is GeoSeries
        assert joined.count() is 4

    def test_sjoin_method2(self):
        left = self.g1
        right = self.g2
        joined = left.sjoin(right)
        assert joined is not None
        assert type(joined) is GeoSeries
        assert joined.count() is 4
