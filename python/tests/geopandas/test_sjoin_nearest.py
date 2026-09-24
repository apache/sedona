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
import pandas as pd
import pytest
import pyspark.pandas as ps
import shapely
from packaging.version import parse as parse_version
from geopandas.testing import assert_geodataframe_equal
from shapely.geometry import LineString, Point

from sedona.spark import geopandas as sgpd
from tests.geopandas.test_geopandas_base import TestGeopandasBase

pytestmark = pytest.mark.skipif(
    parse_version(shapely.__version__) < parse_version("2.0.0"),
    reason="GeoPandas nearest comparisons require Shapely >= 2.0",
)


class TestSpatialJoinNearest(TestGeopandasBase):
    @staticmethod
    def convert(frame):
        with ps.option_context("compute.ops_on_diff_frames", True):
            return sgpd.GeoDataFrame(frame)

    def frames(self):
        left = gpd.GeoDataFrame(
            {"name": ["a", "b"], "geometry": [Point(0, 0), Point(10, 0)]},
            index=pd.Index([5, 5], name="left_id"),
            crs=3857,
        )
        right = gpd.GeoDataFrame(
            {"name": ["near", "far"], "geometry": [Point(1, 0), Point(30, 0)]},
            index=pd.Index([8, 9], name="right_id"),
            crs=3857,
        )
        return left, right

    def nearest(self, left, right, **kwargs):
        join = getattr(sgpd, "sjoin_nearest", None)
        assert callable(join), "The public sjoin_nearest API is missing"
        return join(left, right, **kwargs)

    @pytest.mark.parametrize("max_distance", [None, 1.0, 0.5])
    def test_nearest_matches_geopandas_and_keeps_duplicate_indexes(self, max_distance):
        left, right = self.frames()
        actual = self.nearest(
            self.convert(left),
            self.convert(right),
            max_distance=max_distance,
            distance_col="distance",
        )
        expected = left.sjoin_nearest(
            right, max_distance=max_distance, distance_col="distance"
        )
        assert_geodataframe_equal(
            actual.to_geopandas().sort_values("name_left"),
            expected.sort_values("name_left"),
            check_dtype=False,
        )
        assert actual.crs == left.crs

    @pytest.mark.parametrize("strategy", ["regular", "left", "right"])
    @pytest.mark.parametrize("max_distance", [None, 1.0, 0.5])
    @pytest.mark.parametrize("include_ties, expected", [(False, 1), (True, 2)])
    def test_honors_session_ties_without_changing_configuration(
        self, include_ties, expected, strategy, max_distance
    ):
        key = "spark.sedona.join.knn.includeTieBreakers"
        previous = self.spark.conf.get(key, "false")
        self.spark.conf.set(key, str(include_ties).lower())
        try:
            left = self.convert(gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=3857))
            right = self.convert(
                gpd.GeoDataFrame(
                    {"value": ["west", "east", "far"]},
                    geometry=[Point(-1, 0), Point(1, 0), Point(3, 0)],
                    crs=3857,
                )
            )
            if strategy != "regular":
                frame = left if strategy == "left" else right
                hinted = sgpd.GeoDataFrame(
                    ps.DataFrame(
                        frame._internal.copy(
                            spark_frame=frame._internal.spark_frame.hint("broadcast")
                        )
                    ),
                    geometry="geometry",
                )
                if strategy == "left":
                    left = hinted
                else:
                    right = hinted
            actual = left.sjoin_nearest(
                right, distance_col="distance", max_distance=max_distance
            )
            rows = actual.to_geopandas()
            assert len(rows) == (0 if max_distance == 0.5 else expected)
            assert set(rows["distance"]) == (set() if max_distance == 0.5 else {1.0})
            assert set(rows["value"]).issubset({"west", "east"})
            assert self.spark.conf.get(key) == str(include_ties).lower()
            plan = (
                actual._internal.spark_frame._jdf.queryExecution()
                .executedPlan()
                .toString()
            )
            expected_plan = {
                "regular": "KNNJoin",
                "left": "BroadcastQuerySideKNNJoin",
                "right": "BroadcastObjectSideKNNJoin",
            }[strategy]
            assert expected_plan in plan
            assert "CartesianProduct" not in plan
            assert "PythonUDF" not in plan
        finally:
            self.spark.conf.set(key, previous)

    @pytest.mark.parametrize(
        "side, geometries",
        [
            ("left", []),
            ("right", []),
            ("left", [Point(), None]),
            ("right", [Point(), None]),
        ],
    )
    def test_empty_inputs_keep_schema_and_crs(self, side, geometries):
        left, right = self.frames()
        # Spark 3.5 infers a UDT from the first value, so put the empty Point
        # before None when constructing the fixture. Both still cannot match.
        empty = gpd.GeoDataFrame(
            {"name": ["x"] * len(geometries)}, geometry=geometries, crs=3857
        )
        empty.index.name = f"{side}_id"
        if side == "left":
            left = empty
        else:
            right = empty
        actual = self.nearest(
            self.convert(left), self.convert(right), distance_col="distance"
        )
        expected = left.sjoin_nearest(right, distance_col="distance")
        assert_geodataframe_equal(
            actual.to_geopandas(), expected, check_dtype=False, check_index_type=False
        )
        assert actual.crs == left.crs

    def test_null_empty_and_special_column_names(self):
        left = gpd.GeoDataFrame(
            {
                "a.b": ["keep", "null", "empty"],
                "geom left": [Point(0, 0), None, Point()],
            },
            geometry="geom left",
            crs=3857,
        )
        right = gpd.GeoDataFrame(
            {
                "a.b": ["empty", "null", "near"],
                "geom right": [Point(), None, Point(1, 0)],
            },
            geometry="geom right",
            crs=3857,
        )
        actual = self.nearest(
            self.convert(left), self.convert(right), distance_col="d.m`"
        )
        expected = left.sjoin_nearest(right, distance_col="d.m`")
        assert_geodataframe_equal(actual.to_geopandas(), expected, check_dtype=False)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"how": "left"},
            {"how": "right"},
            {"exclusive": True},
            {"lsuffix": "l"},
            {"rsuffix": "r"},
        ],
    )
    def test_unsupported_options_raise(self, kwargs):
        left, right = self.frames()
        with pytest.raises(NotImplementedError):
            self.nearest(self.convert(left), self.convert(right), **kwargs)

    @pytest.mark.parametrize("distance", [0, -1, float("nan"), float("inf"), "1", True])
    def test_invalid_max_distance_raises(self, distance):
        left, right = self.frames()
        with pytest.raises(ValueError):
            self.nearest(self.convert(left), self.convert(right), max_distance=distance)

    @pytest.mark.parametrize("side", ["left", "right"])
    def test_nonpoint_geometries_raise(self, side):
        left, right = self.frames()
        frame = left if side == "left" else right
        frame.loc[frame.index[0], "geometry"] = LineString([(0, 0), (1, 1)])
        with pytest.raises(NotImplementedError, match="Point"):
            self.nearest(self.convert(left), self.convert(right))

    def test_rejects_multiindex_and_existing_distance_column(self):
        left, right = self.frames()
        with pytest.raises(ValueError, match="distance_col"):
            self.nearest(self.convert(left), self.convert(right), distance_col="name")
        left.index = pd.MultiIndex.from_tuples([(1, "a"), (2, "b")])
        with pytest.raises(NotImplementedError, match="index"):
            self.nearest(self.convert(left), self.convert(right))

    @pytest.mark.parametrize("explicit_none", [False, True])
    def test_empty_result_preserves_source_crs_provenance(self, explicit_none):
        from sedona.spark.geopandas._crs import with_crs_metadata

        raw = self.spark.sql("SELECT ST_SetSRID(ST_Point(0D, 0D), 3857) AS geometry")
        left = sgpd.GeoDataFrame(raw, geometry="geometry")
        if explicit_none:
            internal = left._internal
            field = with_crs_metadata(internal.data_fields[0], None)
            left = sgpd.GeoDataFrame(
                ps.DataFrame(internal.copy(data_fields=[field])), geometry="geometry"
            )
        right = sgpd.GeoDataFrame(
            self.spark.sql("SELECT ST_SetSRID(ST_Point(2D, 0D), 3857) AS geometry"),
            geometry="geometry",
        )
        if explicit_none:
            with pytest.warns(UserWarning, match="CRS mismatch"):
                actual = self.nearest(left, right, max_distance=1)
        else:
            actual = self.nearest(left, right, max_distance=1)
        assert actual.to_geopandas().empty
        assert actual.crs == left.crs
        assert (actual.crs is None) == explicit_none

    def test_duplicate_query_geometries_remain_distinct_rows(self):
        left = self.convert(
            gpd.GeoDataFrame(
                {"row": ["a", "b"]}, geometry=[Point(0, 0)] * 2, index=[7, 7], crs=3857
            )
        )
        right = self.convert(
            gpd.GeoDataFrame(geometry=[Point(0, 0), Point(2, 0)], crs=3857)
        )
        actual = self.nearest(left, right, distance_col="distance").to_geopandas()
        assert sorted(actual["row"]) == ["a", "b"]
        assert list(actual.index) == [7, 7]
        assert set(actual["distance"]) == {0.0}

    def test_rejects_output_label_collisions(self):
        left, right = self.frames()
        left["name_left"] = "existing"
        with pytest.raises(ValueError, match="duplicate output"):
            self.nearest(self.convert(left), self.convert(right))

    def test_named_index_overlap_matches_geopandas(self):
        left, right = self.frames()
        left.index.name = "id"
        right.index.name = "id"
        actual = self.nearest(self.convert(left), self.convert(right)).to_geopandas()
        expected = left.sjoin_nearest(right)
        assert_geodataframe_equal(
            actual.sort_values("name_left"),
            expected.sort_values("name_left"),
            check_dtype=False,
        )

    @pytest.mark.parametrize("max_distance", [None, 0.5])
    def test_secondary_geometry_retains_its_crs_on_empty_results(self, max_distance):
        left = self.convert(gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=3857))
        # Raw Spark columns carry SRIDs without GeoPandas CRS metadata. An
        # empty output must retain the secondary column's source CRS too.
        right = sgpd.GeoDataFrame(
            self.spark.createDataFrame(
                [("POINT (1 0)", "POINT (-73 40)")], ["wkt", "secondary_wkt"]
            ).selectExpr(
                "ST_SetSRID(ST_GeomFromWKT(wkt), 3857) AS geometry",
                "ST_SetSRID(ST_GeomFromWKT(secondary_wkt), 4326) AS secondary",
            ),
            geometry="geometry",
        )
        actual = self.nearest(left, right, max_distance=max_distance)
        assert actual.crs.to_epsg() == 3857
        assert actual["secondary"].crs.to_epsg() == 4326
        rows = actual.to_geopandas()
        assert rows["secondary"].crs.to_epsg() == 4326
        if max_distance is None:
            assert len(rows) == 1
            assert rows["secondary"].iloc[0].equals_exact(Point(-73, 40), 0)
        else:
            assert rows.empty
