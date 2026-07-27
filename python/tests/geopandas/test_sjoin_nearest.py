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
        assert actual.index.names == expected.index.names

    @pytest.mark.parametrize("how", ["inner", "left", "right"])
    def test_multiindex_columns_use_padded_output(self, how):
        columns = pd.MultiIndex.from_tuples(
            [("a", "value"), ("g", "geometry")],
            names=["kind", "field"],
        )
        left = gpd.GeoDataFrame(
            [[1, Point(0, 0)]],
            columns=columns,
            geometry=("g", "geometry"),
            crs="EPSG:3857",
        )
        right = gpd.GeoDataFrame(
            [[2, Point(1, 0)]],
            columns=columns,
            geometry=("g", "geometry"),
            crs="EPSG:3857",
        )
        distance_col = "('a', 'value')_left" if how == "inner" else "distance"

        expected = left.sjoin_nearest(
            right,
            how=how,
            distance_col=distance_col,
        )
        actual = sjoin_nearest(
            self._to_sedona(left),
            self._to_sedona(right),
            how=how,
            distance_col=distance_col,
        )
        collected = actual.to_geopandas()

        assert isinstance(actual.columns, pd.MultiIndex)
        assert actual.columns.names == [None, None]
        expected_padded_columns = pd.MultiIndex.from_tuples(
            [
                label if isinstance(label, tuple) else (label, "")
                for label in expected.columns
            ]
        )
        assert actual.columns.equals(expected_padded_columns)
        expected_active = ("g", "geometry")
        assert actual.active_geometry_name == expected_active
        assert actual.crs == expected.crs

        # GeoPandas flattens reset-index and suffixed labels into a mixed
        # one-level object Index. pandas-on-Spark cannot represent that Index,
        # so compare values after assigning the oracle's labels.
        collected.columns = expected.columns
        collected._geometry_column_name = expected.active_geometry_name
        assert_geodataframe_equal(
            collected,
            expected,
            check_dtype=False,
            check_index_type=False,
        )

    @pytest.mark.parametrize("how", ["inner", "left", "right"])
    def test_column_axis_name_is_dropped(self, how):
        left = gpd.GeoDataFrame(
            {"a": [1], "geometry": [Point(0, 0)]},
            crs="EPSG:3857",
        )
        right = gpd.GeoDataFrame(
            {"b": [2], "geometry": [Point(1, 0)]},
            crs="EPSG:3857",
        )
        left.columns.name = "left_columns"
        right.columns.name = "right_columns"

        expected = left.sjoin_nearest(right, how=how)
        actual = sjoin_nearest(
            self._to_sedona(left),
            self._to_sedona(right),
            how=how,
        )

        self._assert_matches_geopandas(actual, expected)
        assert actual.columns.name is None

    @pytest.mark.parametrize("how", ["inner", "right"])
    def test_tuple_index_name_suffixes_match_geopandas(self, how):
        index = pd.Index([0], name=("idx", "part"))
        left = gpd.GeoDataFrame(
            {"a": [1], "geometry": [Point(0, 0)]},
            index=index,
            crs="EPSG:3857",
        )
        right = gpd.GeoDataFrame(
            {"b": [2], "geometry": [Point(1, 0)]},
            index=index,
            crs="EPSG:3857",
        )

        expected = left.sjoin_nearest(right, how=how)
        actual = sjoin_nearest(
            self._to_sedona(left),
            self._to_sedona(right),
            how=how,
        )

        self._assert_matches_geopandas(actual, expected)
        assert list(actual.columns) == list(expected.columns)
        assert actual.index.names == expected.index.names

    def test_suffix_created_duplicate_labels_are_rejected(self):
        left = gpd.GeoDataFrame(
            {
                "a": [10],
                "a_left": [20],
                "geometry": [Point(0, 0)],
            },
            crs="EPSG:3857",
        )
        right = gpd.GeoDataFrame(
            {
                "a": [30],
                "geometry": [Point(3, 4)],
            },
            crs="EPSG:3857",
        )

        with pytest.warns(FutureWarning):
            left.sjoin_nearest(
                right,
                distance_col="a_left",
            )
        with pytest.raises(ValueError, match="duplicate output columns.*a_left"):
            sjoin_nearest(
                self._to_sedona(left),
                self._to_sedona(right),
                distance_col="a_left",
            )

    def test_multiindex_padding_collision_is_rejected(self):
        left = gpd.GeoDataFrame(
            {"a": [1], "geometry": [Point(0, 0)]},
            crs="EPSG:3857",
        )
        right_columns = pd.MultiIndex.from_tuples([("a", ""), ("g", "geometry")])
        right = gpd.GeoDataFrame(
            [[2, Point(1, 0)]],
            columns=right_columns,
            geometry=("g", "geometry"),
            crs="EPSG:3857",
        )

        expected = left.sjoin_nearest(right)
        assert expected.columns.is_unique
        with pytest.raises(ValueError, match="duplicate output columns.*a"):
            sjoin_nearest(
                self._to_sedona(left),
                self._to_sedona(right),
            )

    @pytest.mark.parametrize(
        "multi_on_left,how",
        [(True, "right"), (False, "inner")],
    )
    def test_mixed_column_depth_preserves_active_geometry(
        self,
        multi_on_left,
        how,
    ):
        columns = pd.MultiIndex.from_tuples(
            [("a", "value"), ("g", "geometry")],
            names=["kind", "field"],
        )
        multi = gpd.GeoDataFrame(
            [[1, Point(0, 0)]],
            columns=columns,
            geometry=("g", "geometry"),
            crs="EPSG:3857",
        )
        plain = gpd.GeoDataFrame(
            {"a": [2], "geometry": [Point(1, 0)]},
            crs="EPSG:3857",
        )
        left, right = (multi, plain) if multi_on_left else (plain, multi)

        expected = left.sjoin_nearest(right, how=how)
        actual = sjoin_nearest(
            self._to_sedona(left),
            self._to_sedona(right),
            how=how,
        )
        collected = actual.to_geopandas()

        assert isinstance(actual.columns, pd.MultiIndex)
        assert actual.active_geometry_name == ("geometry", "")
        assert actual.crs == expected.crs
        collected.columns = expected.columns
        collected._geometry_column_name = expected.active_geometry_name
        assert_geodataframe_equal(
            collected,
            expected,
            check_dtype=False,
            check_index_type=False,
        )

    @pytest.mark.parametrize("how", ["inner", "left"])
    def test_dropped_right_geometry_does_not_conflict_with_left_index(self, how):
        left = gpd.GeoDataFrame(
            {"geometry": [Point(0, 0)]},
            crs="EPSG:3857",
        )
        right = gpd.GeoDataFrame(
            {
                "x": [1],
                "index_left": [Point(1, 0)],
            },
            geometry="index_left",
            crs="EPSG:3857",
        )

        expected = left.sjoin_nearest(right, how=how)
        actual = sjoin_nearest(
            self._to_sedona(left),
            self._to_sedona(right),
            how=how,
        )
        self._assert_matches_geopandas(actual, expected)

    def test_dropped_left_geometry_does_not_conflict_with_right_index(self):
        left = gpd.GeoDataFrame(
            {
                "x": [1],
                "index_right": [Point(0, 0)],
            },
            geometry="index_right",
            crs="EPSG:3857",
        )
        right = gpd.GeoDataFrame(
            {"geometry": [Point(1, 0)]},
            crs="EPSG:3857",
        )

        expected = left.sjoin_nearest(right, how="right")
        actual = sjoin_nearest(
            self._to_sedona(left),
            self._to_sedona(right),
            how="right",
        )
        self._assert_matches_geopandas(actual, expected)

    @pytest.mark.parametrize(
        "column_name,how",
        [("index_left", "inner"), ("index_right", "right")],
    )
    def test_generated_index_detects_multiindex_first_level_collision(
        self,
        column_name,
        how,
    ):
        columns = pd.MultiIndex.from_tuples([(column_name, "value"), ("g", "geometry")])
        conflicting = gpd.GeoDataFrame(
            [[1, Point(0, 0)]],
            columns=columns,
            geometry=("g", "geometry"),
            crs="EPSG:3857",
        )
        plain = gpd.GeoDataFrame(
            {"geometry": [Point(1, 0)]},
            crs="EPSG:3857",
        )
        left, right = (
            (conflicting, plain)
            if column_name == "index_left"
            else (plain, conflicting)
        )

        with pytest.raises(ValueError, match=column_name):
            left.sjoin_nearest(right, how=how)
        with pytest.raises(ValueError, match=column_name):
            sjoin_nearest(
                self._to_sedona(left),
                self._to_sedona(right),
                how=how,
            )

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
