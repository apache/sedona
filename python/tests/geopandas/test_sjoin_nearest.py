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

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import pyspark.pandas as ps
import shapely
from geopandas.testing import assert_geodataframe_equal
from packaging.version import parse as parse_version
from shapely.geometry import LineString, Point

from sedona.spark.geopandas import GeoDataFrame, sjoin_nearest
from sedona.spark.sql import st_functions as stf
from tests.geopandas.test_geopandas_base import TestGeopandasBase

pytestmark = pytest.mark.skipif(
    parse_version(shapely.__version__) < parse_version("2.0.0"),
    reason=f"Tests require shapely>=2.0.0, but found v{shapely.__version__}",
)


class TestSpatialJoinNearest(TestGeopandasBase):
    @staticmethod
    def _to_sedona(frame):
        with ps.option_context("compute.ops_on_diff_frames", True):
            return GeoDataFrame(frame)

    @staticmethod
    def _frames():
        left = gpd.GeoDataFrame(
            {
                "key": ["a", "b"],
                "same": [1, 2],
                "geometry": [Point(0, 0), Point(10, 0)],
            },
            index=pd.Index([5, 6], name="left_id"),
            crs="EPSG:3857",
        )
        right = gpd.GeoDataFrame(
            {
                "same": [3, 4, 5],
                "value": ["west", "east", "far"],
                "geometry": [Point(-1, 0), Point(1, 0), Point(30, 0)],
            },
            index=pd.Index([7, 8, 9], name="right_id"),
            crs="EPSG:3857",
        )
        return left, right

    @staticmethod
    def _assert_matches_geopandas(actual, expected):
        assert isinstance(actual, GeoDataFrame)
        assert_geodataframe_equal(
            actual.to_geopandas(),
            expected,
            check_dtype=False,
            check_index_type=False,
        )

    @pytest.mark.parametrize("how", ["inner", "left", "right"])
    def test_ties_join_modes_distance_and_method(self, how):
        left, right = self._frames()
        expected = left.sjoin_nearest(
            right,
            how=how,
            distance_col="distance",
        )
        left_sgpd = self._to_sedona(left)
        right_sgpd = self._to_sedona(right)

        actual = sjoin_nearest(
            left_sgpd,
            right_sgpd,
            how=how,
            distance_col="distance",
        )
        method_result = left_sgpd.sjoin_nearest(
            right_sgpd,
            how=how,
            distance_col="distance",
        )

        self._assert_matches_geopandas(actual, expected)
        self._assert_matches_geopandas(method_result, expected)
        assert actual.crs == expected.crs

    @pytest.mark.parametrize("how", ["inner", "left", "right"])
    def test_max_distance(self, how):
        left, right = self._frames()
        expected = left.sjoin_nearest(
            right,
            how=how,
            max_distance=2.0,
            distance_col="distance",
        )
        actual = sjoin_nearest(
            self._to_sedona(left),
            self._to_sedona(right),
            how=how,
            max_distance=2.0,
            distance_col="distance",
        )
        self._assert_matches_geopandas(actual, expected)

    @pytest.mark.parametrize("how", ["inner", "right"])
    def test_duplicate_multiindex_and_index_name_suffixes(self, how):
        left_index = pd.MultiIndex.from_tuples(
            [(1, "a"), (1, "a")],
            names=["id", "part"],
        )
        right_index = pd.MultiIndex.from_tuples(
            [(2, "b")],
            names=["id", "part"],
        )
        left = gpd.GeoDataFrame(
            {
                "shared": [1, 2],
                "geometry": [Point(0, 0), Point(0, 0)],
            },
            index=left_index,
            crs="EPSG:3857",
        )
        right = gpd.GeoDataFrame(
            {"shared": [3], "geometry": [Point(1, 0)]},
            index=right_index,
            crs="EPSG:3857",
        )

        expected = left.sjoin_nearest(right, how=how)
        actual = sjoin_nearest(
            self._to_sedona(left),
            self._to_sedona(right),
            how=how,
        )
        self._assert_matches_geopandas(actual, expected)
        assert actual.index.names == expected.index.names

    def test_exclusive_uses_topological_equality(self):
        left = gpd.GeoDataFrame(
            {"geometry": [LineString([(0, 0), (1, 0)])]},
            crs="EPSG:3857",
        )
        right = gpd.GeoDataFrame(
            {
                "value": ["reversed", "next"],
                "geometry": [
                    LineString([(1, 0), (0, 0)]),
                    LineString([(0, 2), (1, 2)]),
                ],
            },
            crs="EPSG:3857",
        )

        expected = left.sjoin_nearest(
            right,
            exclusive=True,
            distance_col="distance",
        )
        actual = sjoin_nearest(
            self._to_sedona(left),
            self._to_sedona(right),
            exclusive=True,
            distance_col="distance",
        )
        self._assert_matches_geopandas(actual, expected)

    def test_exclusive_self_join(self):
        frame = gpd.GeoDataFrame(
            {
                "value": ["a", "b", "c"],
                "geometry": [Point(0, 0), Point(2, 0), Point(5, 0)],
            },
            crs="EPSG:3857",
        )
        expected = frame.sjoin_nearest(
            frame,
            exclusive=True,
            distance_col="distance",
        )
        sedona_frame = self._to_sedona(frame)
        actual = sedona_frame.sjoin_nearest(
            sedona_frame,
            exclusive=True,
            distance_col="distance",
        )
        self._assert_matches_geopandas(actual, expected)

    @pytest.mark.parametrize("how", ["inner", "left", "right"])
    def test_null_and_empty_geometries(self, how):
        left = gpd.GeoDataFrame(
            {
                "value": ["point", "null", "empty"],
                "geometry": [Point(0, 0), None, Point()],
            },
            crs="EPSG:3857",
        )
        right = gpd.GeoDataFrame(
            {"candidate": [1], "geometry": [Point(1, 0)]},
            crs="EPSG:3857",
        )

        expected = left.sjoin_nearest(right, how=how, distance_col="distance")
        actual = sjoin_nearest(
            self._to_sedona(left),
            self._to_sedona(right),
            how=how,
            distance_col="distance",
        )
        self._assert_matches_geopandas(actual, expected)

    @pytest.mark.parametrize("all_null", [False, True])
    def test_empty_and_all_null_candidate_frames(self, all_null):
        left = gpd.GeoDataFrame(
            {"value": [1], "geometry": [Point(0, 0)]},
            crs="EPSG:3857",
        )
        if all_null:
            right = gpd.GeoDataFrame(
                {"candidate": [1, 2], "geometry": [None, None]},
                crs="EPSG:3857",
            )
        else:
            right = gpd.GeoDataFrame(
                {
                    "candidate": pd.Series(dtype="int64"),
                    "geometry": gpd.GeoSeries([], crs="EPSG:3857"),
                },
                crs="EPSG:3857",
            )

        for how in ("inner", "left", "right"):
            expected = left.sjoin_nearest(
                right,
                how=how,
                distance_col="distance",
            )
            actual = sjoin_nearest(
                self._to_sedona(left),
                self._to_sedona(right),
                how=how,
                distance_col="distance",
            )
            self._assert_matches_geopandas(actual, expected)
            assert actual.crs == expected.crs

    def test_custom_geometry_name_crs_srid_and_plan(self):
        left, right = self._frames()
        left = left.rename_geometry("left_geometry")
        right = right.rename_geometry("right_geometry")
        actual = sjoin_nearest(
            self._to_sedona(left),
            self._to_sedona(right),
        )

        assert actual.active_geometry_name == "left_geometry"
        assert actual.crs == left.crs
        srids = actual._internal.spark_frame.select(
            stf.ST_SRID(actual.geometry.spark.column).alias("srid")
        ).collect()
        assert {row.srid for row in srids if row.srid is not None} == {3857}

        plan = (
            actual._internal.spark_frame._jdf.queryExecution().executedPlan().toString()
        )
        assert "KNNJoin" in plan
        assert "CartesianProduct" not in plan
        assert "BatchEvalPython" not in plan
        assert "PythonUDF" not in plan

    def test_distance_column_can_replace_active_geometry(self):
        left, right = self._frames()
        expected = left.sjoin_nearest(right, distance_col="geometry")
        with pytest.warns(UserWarning, match="does not contain geometry"):
            actual = sjoin_nearest(
                self._to_sedona(left),
                self._to_sedona(right),
                distance_col="geometry",
            )

        assert actual.active_geometry_name is None
        pd.testing.assert_frame_equal(
            pd.DataFrame(actual.to_geopandas()),
            pd.DataFrame(expected),
            check_dtype=False,
            check_index_type=False,
        )

    def test_validation_and_crs_warnings(self):
        left, right = self._frames()
        left_sgpd = self._to_sedona(left)
        right_sgpd = self._to_sedona(right)

        with pytest.raises(ValueError, match="expected to be in"):
            sjoin_nearest(left_sgpd, right_sgpd, how="outer")
        with pytest.raises(ValueError, match="greater than 0"):
            sjoin_nearest(left_sgpd, right_sgpd, max_distance=0)
        with pytest.raises(TypeError, match="must be a number"):
            sjoin_nearest(left_sgpd, right_sgpd, max_distance="1")
        one_result = sjoin_nearest(left_sgpd, right_sgpd, exclusive=1)
        numpy_bool_result = sjoin_nearest(
            left_sgpd,
            right_sgpd,
            exclusive=np.bool_(True),
        )
        self._assert_matches_geopandas(
            one_result,
            left.sjoin_nearest(right, exclusive=1),
        )
        self._assert_matches_geopandas(
            numpy_bool_result,
            left.sjoin_nearest(right, exclusive=np.bool_(True)),
        )
        with pytest.raises(ValueError, match="must be boolean"):
            sjoin_nearest(left_sgpd, right_sgpd, exclusive=2)
        with pytest.raises(TypeError, match="must be a string"):
            sjoin_nearest(left_sgpd, right_sgpd, distance_col=1)

        geographic_left = self._to_sedona(left.to_crs(4326))
        geographic_right = self._to_sedona(right.to_crs(4326))
        with pytest.warns(UserWarning, match="geographic CRS") as warnings_seen:
            sjoin_nearest(geographic_left, geographic_right)
        assert (
            sum("geographic CRS" in str(warning.message) for warning in warnings_seen)
            == 2
        )

        mismatched = self._to_sedona(right.to_crs(32631))
        with pytest.warns(UserWarning, match="CRS mismatch"):
            sjoin_nearest(left_sgpd, mismatched)

        no_crs = self._to_sedona(right.set_crs(None, allow_override=True))
        with pytest.warns(UserWarning, match="Right CRS: None"):
            sjoin_nearest(left_sgpd, no_crs)
