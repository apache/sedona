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

import geopandas as gpd
import pandas as pd
import pytest
from geopandas.testing import assert_geoseries_equal
from shapely.geometry import Point, Polygon, box
from tests.test_base import TestBase

from sedona.spark import geopandas as sgpd

# The prefix is JVM-wide and fixed by the first Sedona registrator, so these tests need a
# pytest process of their own whose session was built with SEDONA_TEST_FUNCTION_PREFIX.
PREFIX = os.getenv("SEDONA_TEST_FUNCTION_PREFIX")
pytestmark = pytest.mark.skipif(not PREFIX, reason="needs SEDONA_TEST_FUNCTION_PREFIX")


def square(x, y, size=1.0):
    return Polygon([(x, y), (x + size, y), (x + size, y + size), (x, y + size)])


def sgpd_frame(**columns):
    return sgpd.GeoDataFrame(gpd.GeoDataFrame(columns))


class TestPrefixGeopandas(TestBase):
    def test_sjoin_intersects(self):
        left = sgpd_frame(geometry=[square(0, 0), square(0.5, 0.5)], left_id=[1, 2])
        right = sgpd_frame(
            geometry=[Point(0.75, 0.75), Point(10, 10)], right_id=[10, 20]
        )
        # Both squares contain (0.75, 0.75); the point (10, 10) matches neither.
        joined = sgpd.sjoin(left, right, predicate="intersects").to_pandas()
        assert sorted(zip(joined["left_id"], joined["right_id"])) == [(1, 10), (2, 10)]

    def test_sjoin_dwithin(self):
        left = sgpd_frame(geometry=[Point(0, 0)], left_id=[1])
        right = sgpd_frame(geometry=[Point(1, 0), Point(5, 0)], right_id=[10, 20])
        joined = sgpd.sjoin(left, right, predicate="dwithin", distance=1.5)
        assert list(joined.to_pandas()["right_id"]) == [10]

    def test_sjoin_on_attribute(self):
        left = sgpd_frame(geometry=[Point(0, 0)], key=["a"])
        right = sgpd_frame(geometry=[Point(0, 0)], key=["b"])
        joined = sgpd.sjoin(left, right, predicate="intersects", on_attribute=["key"])
        assert len(joined.to_pandas()) == 0

    def test_sjoin_nearest(self):
        left = sgpd_frame(geometry=[Point(0, 0), Point(10, 10)], left_id=[1, 2])
        right = sgpd_frame(geometry=[Point(1, 1), Point(9, 9)], right_id=[10, 20])
        joined = sgpd.sjoin_nearest(left, right).to_pandas()
        assert sorted(zip(joined["left_id"], joined["right_id"])) == [(1, 10), (2, 20)]

    def test_invalid_coverage_edges(self):
        series = sgpd.GeoSeries(gpd.GeoSeries([square(0, 0), square(1.1, 0)]))
        # The squares are 0.1 apart: a gap_width of 0.5 flags the facing edges.
        assert series.is_valid_coverage(gap_width=0.5) is False
        edges = series.invalid_coverage_edges(gap_width=0.5).to_geopandas()
        assert len(edges) == 2
        assert not edges.is_empty.any()

    def test_simplify_coverage(self):
        # The two polygons of test_simplify_coverage.py's shared-boundary test: the shared
        # vertex (1.1, 0.5) is removed from both, which leaves two unit squares.
        left = Polygon([(0, 0), (1, 0), (1.1, 0.5), (1, 1), (0, 1)])
        right = Polygon([(1, 0), (2, 0), (2, 1), (1, 1), (1.1, 0.5)])
        source = sgpd.GeoSeries([left, right], crs=3857)

        result = source.simplify_coverage(0.3, simplify_boundary=False).to_geopandas()

        expected = gpd.GeoSeries([box(0, 0, 1, 1), box(1, 0, 2, 1)], crs=3857)
        assert_geoseries_equal(result, expected, normalize=True)
