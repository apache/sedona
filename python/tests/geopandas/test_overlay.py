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
from packaging.version import parse as parse_version
from shapely.geometry import LineString, MultiPolygon, Point, Polygon, box
from shapely.geometry.base import BaseGeometry

import sedona.spark.geopandas as sgpd
from sedona.spark.geopandas import GeoDataFrame
from sedona.spark.sql import st_functions as stf
from sedona.spark.sql.types import GeometryType
from tests.geopandas.test_geopandas_base import TestGeopandasBase

GEOPANDAS_GE_10 = parse_version(gpd.__version__) >= parse_version("1.0.0")
GEOPANDAS_GE_11 = parse_version(gpd.__version__) >= parse_version("1.1.0")


class TestDistributedOverlay(TestGeopandasBase):
    @staticmethod
    def _value_equal(left, right, check_geom_type=True):
        if isinstance(left, BaseGeometry) or isinstance(right, BaseGeometry):
            return (
                isinstance(left, BaseGeometry)
                and isinstance(right, BaseGeometry)
                and (not check_geom_type or left.geom_type == right.geom_type)
                and left.equals(right)
            )
        if pd.isna(left) and pd.isna(right):
            return True
        return left == right

    @classmethod
    def _assert_overlay_equal(
        cls, actual, expected, check_dtype=True, check_geom_type=True
    ):
        assert isinstance(actual, GeoDataFrame)
        actual_local = actual.to_geopandas()
        assert list(actual_local.index) == list(range(len(actual_local)))
        actual_local = actual_local.reset_index(drop=True)
        expected_local = expected.reset_index(drop=True)
        assert list(actual_local.columns) == list(expected_local.columns)
        if check_dtype:
            assert [str(dtype) for dtype in actual_local.dtypes] == [
                str(dtype) for dtype in expected_local.dtypes
            ]
        assert actual.active_geometry_name == expected.geometry.name
        assert actual.crs == expected.crs
        assert len(actual_local) == len(expected_local)

        unmatched = list(range(len(actual_local)))
        for _, expected_row in expected_local.iterrows():
            for position in unmatched:
                actual_row = actual_local.iloc[position]
                if all(
                    cls._value_equal(
                        actual_row[column],
                        expected_row[column],
                        check_geom_type=check_geom_type,
                    )
                    for column in expected_local.columns
                ):
                    unmatched.remove(position)
                    break
            else:
                raise AssertionError(
                    f"No distributed overlay row matches {expected_row.to_dict()}"
                )
        assert not unmatched
        return actual_local

    @staticmethod
    def _assert_srid(frame, expected):
        internal = frame._internal.resolved_copy
        geometry = internal.spark_column_for((frame.active_geometry_name,))
        srids = {
            row.srid
            for row in internal.spark_frame.select(
                stf.ST_SRID(geometry).alias("srid")
            ).collect()
            if row.srid is not None
        }
        assert srids == {expected}

    @pytest.mark.parametrize(
        "how",
        [
            "intersection",
            "difference",
            "identity",
            "symmetric_difference",
            "union",
        ],
    )
    def test_all_modes_match_geopandas_with_duplicate_multiindex(self, how):
        left_index = pd.MultiIndex.from_tuples(
            [("left", 1), ("left", 1)], names=["group", "position"]
        )
        right_index = pd.Index(["mask"], name="source")
        left_local = gpd.GeoDataFrame(
            {
                "value": [1, 2],
                "left_only": ["a", "b"],
                "geom": [box(0, 0, 2, 2), box(3, 3, 5, 5)],
            },
            geometry="geom",
            index=left_index,
            crs=4326,
        )
        right_local = gpd.GeoDataFrame(
            {
                "value": [10],
                "right_only": ["mask"],
                "shape": [box(1, 1, 4, 4)],
            },
            geometry="shape",
            index=right_index,
            crs=4326,
        )
        left = GeoDataFrame(left_local)
        right = GeoDataFrame(right_local)

        actual = (
            sgpd.overlay(left, right, how=how, keep_geom_type=False)
            if how in {"intersection", "union"}
            else left.overlay(right, how=how, keep_geom_type=False)
        )
        expected = gpd.overlay(left_local, right_local, how=how, keep_geom_type=False)

        self._assert_overlay_equal(
            actual,
            expected,
            # GeoPandas < 1.1 built identity from union and widened left-side
            # dtypes. The distributed API follows its 1.1+ identity semantics.
            check_dtype=how != "identity" or GEOPANDAS_GE_11,
        )
        self._assert_srid(actual, 4326)

    def test_keep_geom_type_filters_lower_dimension_lazily(self):
        left_local = gpd.GeoDataFrame(
            {"name": ["polygon"], "geometry": [box(0, 0, 1, 1)]}, crs=3857
        )
        right_local = gpd.GeoDataFrame(
            {"name": ["touch"], "geometry": [box(1, 0, 2, 1)]}, crs=3857
        )
        left = GeoDataFrame(left_local)
        right = GeoDataFrame(right_local)

        kept = left.overlay(right)
        retained = left.overlay(right, keep_geom_type=False)
        retained_numpy_false = left.overlay(right, keep_geom_type=np.bool_(False))

        assert len(kept) == 0
        retained_local = retained.to_geopandas()
        assert retained_local.geom_type.tolist() == ["LineString"]
        retained_numpy_false_local = retained_numpy_false.to_geopandas()
        assert retained_numpy_false_local.geom_type.tolist() == ["LineString"]

    def test_keep_geom_type_extracts_polygon_from_collection(self):
        left_local = gpd.GeoDataFrame(
            {"geometry": [MultiPolygon([box(0, 0, 1, 1), box(2, 0, 3, 1)])]},
            crs=4326,
        )
        right_local = gpd.GeoDataFrame({"geometry": [box(0.5, -0.5, 2, 1.5)]}, crs=4326)
        left = GeoDataFrame(left_local)
        right = GeoDataFrame(right_local)

        expected_kept = gpd.overlay(left_local, right_local, keep_geom_type=True)
        expected_all = gpd.overlay(left_local, right_local, keep_geom_type=False)
        kept_default = left.overlay(right)
        kept_explicit = left.overlay(right, keep_geom_type=True)
        retained = left.overlay(right, keep_geom_type=False)

        self._assert_overlay_equal(kept_default, expected_kept)
        self._assert_overlay_equal(kept_explicit, expected_kept)
        retained_local = self._assert_overlay_equal(retained, expected_all)
        assert retained_local.geom_type.tolist() == ["GeometryCollection"]

    def test_make_valid_and_validation(self):
        invalid = Polygon([(0, 0), (2, 2), (0, 2), (2, 0), (0, 0)])
        left = GeoDataFrame(gpd.GeoDataFrame({"geometry": [invalid]}, crs=4326))
        right = GeoDataFrame(
            gpd.GeoDataFrame({"geometry": [box(-1, -1, 3, 3)]}, crs=4326)
        )

        with pytest.raises(ValueError, match="make_valid=False"):
            left.overlay(right, make_valid=False)

        result = left.overlay(right, make_valid=True)
        result_local = result.to_geopandas()
        assert len(result_local) == 1
        assert result_local.geometry.iloc[0].is_valid
        assert result_local.geom_type.iloc[0] in {"Polygon", "MultiPolygon"}
        self._assert_srid(result, 4326)

    def test_difference_unions_multiple_matched_masks_per_source(self):
        left_local = gpd.GeoDataFrame(
            {"feature": [1], "geometry": [box(0, 0, 5, 5)]}, crs=4326
        )
        right_local = gpd.GeoDataFrame(
            {
                "mask": ["west", "east"],
                "geometry": [box(0, 0, 2, 5), box(3, 0, 5, 5)],
            },
            crs=4326,
        )

        result = GeoDataFrame(left_local).overlay(
            GeoDataFrame(right_local), how="difference", keep_geom_type=False
        )
        expected = gpd.overlay(
            left_local, right_local, how="difference", keep_geom_type=False
        )

        result_local = self._assert_overlay_equal(result, expected)
        assert result_local.geometry.iloc[0].equals(box(2, 0, 3, 5))

    @pytest.mark.parametrize(
        "how", ["difference", "identity", "symmetric_difference", "union"]
    )
    def test_disjoint_single_part_multipolygon_preserves_geometry_type(self, how):
        left_local = gpd.GeoDataFrame(
            {"geometry": [MultiPolygon([box(0, 0, 1, 1)])]}, crs=4326
        )
        right_local = gpd.GeoDataFrame({"geometry": [box(2, 0, 3, 1)]}, crs=4326)

        result = GeoDataFrame(left_local).overlay(
            GeoDataFrame(right_local), how=how, keep_geom_type=False
        )
        expected = gpd.overlay(left_local, right_local, how=how, keep_geom_type=False)

        # GeoPandas 0.13 unwraps the single-part MultiPolygon during overlay
        # output repair. The distributed API follows GeoPandas 1.0+ and must
        # preserve it, while topology still matches the legacy oracle.
        result_local = self._assert_overlay_equal(
            result,
            expected,
            check_geom_type=GEOPANDAS_GE_10,
        )
        expected_types = ["MultiPolygon"]
        if how in ("symmetric_difference", "union"):
            expected_types.append("Polygon")
        assert sorted(result_local.geom_type) == sorted(expected_types)

    def test_common_mode_dtype_promotion_matches_geopandas(self):
        left_local = gpd.GeoDataFrame(
            {
                "left_int": [1],
                "left_bool": [True],
                "geometry": [box(0, 0, 1, 1)],
            },
            crs=4326,
        )
        right_local = gpd.GeoDataFrame(
            {
                "right_int": [2],
                "right_bool": [False],
                "geometry": [box(5, 5, 6, 6)],
            },
            crs=4326,
        )
        left = GeoDataFrame(left_local)
        right = GeoDataFrame(right_local)

        intersection = left.overlay(right, keep_geom_type=False)
        difference = left.overlay(right, how="difference", keep_geom_type=False)
        identity = left.overlay(right, how="identity", keep_geom_type=False)
        union = left.overlay(right, how="union", keep_geom_type=False)

        assert str(intersection["left_int"].dtype) == "int64"
        assert str(intersection["left_bool"].dtype) == "bool"
        assert str(difference["left_int"].dtype) == "int64"
        assert str(difference["left_bool"].dtype) == "bool"
        assert str(identity["left_int"].dtype) == "int64"
        assert str(identity["left_bool"].dtype) == "bool"
        assert str(identity["right_int"].dtype) == "float64"
        assert str(identity["right_bool"].dtype) == "object"
        for name in ("left_int", "right_int"):
            assert str(union[name].dtype) == "float64"
        for name in ("left_bool", "right_bool"):
            assert str(union[name].dtype) == "object"

        expected = gpd.overlay(
            left_local, right_local, how="union", keep_geom_type=False
        )
        union_local = self._assert_overlay_equal(union, expected)
        assert union_local.dtypes.equals(expected.reset_index(drop=True).dtypes)

    def test_common_modes_preserve_nullable_extension_dtypes(self):
        left_local = gpd.GeoDataFrame(
            {
                "left_int": pd.Series([1], dtype="Int64"),
                "left_bool": pd.Series([True], dtype="boolean"),
                "geometry": [box(0, 0, 1, 1)],
            },
            crs=4326,
        )
        right_local = gpd.GeoDataFrame(
            {
                "right_int": pd.Series([2], dtype="Int64"),
                "right_bool": pd.Series([False], dtype="boolean"),
                "geometry": [box(5, 5, 6, 6)],
            },
            crs=4326,
        )

        result = GeoDataFrame(left_local).overlay(
            GeoDataFrame(right_local), how="union", keep_geom_type=False
        )
        expected = gpd.overlay(
            left_local, right_local, how="union", keep_geom_type=False
        )

        result_local = self._assert_overlay_equal(result, expected)
        assert result_local.dtypes.equals(expected.reset_index(drop=True).dtypes)
        assert str(result["left_int"].dtype) == "Int64"
        assert str(result["right_int"].dtype) == "Int64"
        assert str(result["left_bool"].dtype) == "boolean"
        assert str(result["right_bool"].dtype) == "boolean"

    def test_null_empty_disjoint_and_no_crs_reset_srid(self):
        left_local = gpd.GeoDataFrame(
            {
                "value": [3, 2, 1],
                "geometry": [box(0, 0, 1, 1), Polygon(), None],
            }
        )
        right_local = gpd.GeoDataFrame(
            {"mask": [1], "geometry": [box(5, 5, 6, 6)]}, crs=4326
        )
        left = GeoDataFrame(left_local)
        right = GeoDataFrame(right_local)

        with pytest.warns(UserWarning, match="CRS mismatch"):
            result = left.overlay(right, how="union", keep_geom_type=False)

        result_local = result.to_geopandas()
        assert result.crs is None
        assert result_local.geometry.isna().sum() == 1
        self._assert_srid(result, 0)

    def test_empty_input_preserves_schema_geometry_name_and_crs(self):
        empty_local = gpd.GeoDataFrame(
            {
                "value": pd.Series(dtype="int64"),
                "geom": gpd.GeoSeries([], crs=4326),
            },
            geometry="geom",
            crs=4326,
        )
        right_local = gpd.GeoDataFrame(
            {"mask": [1], "shape": [box(0, 0, 1, 1)]},
            geometry="shape",
            crs=4326,
        )
        left = GeoDataFrame(empty_local)
        right = GeoDataFrame(right_local)

        intersection = left.overlay(right, keep_geom_type=False)
        difference = left.overlay(right, how="difference", keep_geom_type=False)

        assert len(intersection) == len(difference) == 0
        assert list(intersection.columns) == ["value", "mask", "geometry"]
        assert intersection.active_geometry_name == "geometry"
        assert list(difference.columns) == ["value", "geom"]
        assert difference.active_geometry_name == "geom"
        assert intersection.crs == difference.crs == left.crs
        assert isinstance(
            intersection.geometry._internal.data_fields[0].spark_type, GeometryType
        )
        assert isinstance(
            difference.geometry._internal.data_fields[0].spark_type, GeometryType
        )

    def test_point_line_and_multipart_families(self):
        point_left = gpd.GeoDataFrame(
            {"geometry": [Point(0, 0), Point(2, 2)]}, crs=4326
        )
        point_right = gpd.GeoDataFrame({"geometry": [Point(0, 0)]}, crs=4326)
        self._assert_overlay_equal(
            GeoDataFrame(point_left).overlay(GeoDataFrame(point_right)),
            gpd.overlay(point_left, point_right),
        )

        line_left = gpd.GeoDataFrame(
            {"geometry": [LineString([(0, 0), (2, 0)])]}, crs=4326
        )
        line_right = gpd.GeoDataFrame(
            {"geometry": [LineString([(1, -1), (1, 1)])]}, crs=4326
        )
        self._assert_overlay_equal(
            GeoDataFrame(line_left).overlay(
                GeoDataFrame(line_right), keep_geom_type=False
            ),
            gpd.overlay(line_left, line_right, keep_geom_type=False),
        )

        multi_left = gpd.GeoDataFrame(
            {"geometry": [MultiPolygon([box(0, 0, 2, 2), box(3, 0, 5, 2)])]},
            crs=4326,
        )
        multi_right = gpd.GeoDataFrame({"geometry": [box(1, -1, 4, 3)]}, crs=4326)
        self._assert_overlay_equal(
            GeoDataFrame(multi_left).overlay(GeoDataFrame(multi_right)),
            gpd.overlay(multi_left, multi_right),
        )

    def test_inactive_geometry_numeric_labels_and_suffixes(self):
        left_local = gpd.GeoDataFrame(
            {
                1: [10],
                "inactive": gpd.GeoSeries([Point(20, 20)], crs=4326),
                "geometry": ["reserved output name"],
                "geom": [box(0, 0, 2, 2)],
            },
            geometry="geom",
            crs=4326,
        )
        right_local = gpd.GeoDataFrame(
            {1: [20], "shape": [box(1, 1, 3, 3)]},
            geometry="shape",
            crs=4326,
        )

        left = GeoDataFrame(left_local)
        inactive_crs = left["inactive"].crs
        result = left.overlay(GeoDataFrame(right_local))
        expected = gpd.overlay(left_local, right_local)

        self._assert_overlay_equal(result, expected)
        assert "geometry" in result.columns
        assert isinstance(result["inactive"], sgpd.GeoSeries)
        assert result["inactive"].crs == inactive_crs

    @pytest.mark.parametrize(
        "how",
        [
            "intersection",
            "difference",
            "identity",
            "symmetric_difference",
            "union",
        ],
    )
    def test_both_inactive_geometry_columns_match_geopandas(self, how):
        left_local = gpd.GeoDataFrame(
            {
                "geometry": gpd.GeoSeries([Point(10, 10)], crs=4326),
                "left": [1],
                "geom": [box(0, 0, 2, 2)],
            },
            geometry="geom",
            crs=4326,
        )
        right_local = gpd.GeoDataFrame(
            {
                "geometry": gpd.GeoSeries([Point(20, 20)], crs=4326),
                "right": [2],
                "shape": [box(1, 1, 3, 3)],
            },
            geometry="shape",
            crs=4326,
        )

        result = GeoDataFrame(left_local).overlay(
            GeoDataFrame(right_local), how=how, keep_geom_type=False
        )
        expected = gpd.overlay(left_local, right_local, how=how, keep_geom_type=False)
        result_local = self._assert_overlay_equal(
            result,
            expected,
            check_dtype=how != "identity" or GEOPANDAS_GE_11,
        )

        if how == "difference":
            assert list(result.columns) == ["geometry", "left", "geom"]
            assert result_local["geometry"].notna().all()
        elif how == "symmetric_difference":
            assert list(result.columns) == ["left", "right", "geometry"]
        else:
            assert list(result.columns) == [
                "geometry_1",
                "left",
                "geometry_2",
                "right",
                "geometry",
            ]
            assert result_local["geometry_1"].notna().sum() == 1
            assert result_local["geometry_2"].notna().sum() == 1

    @pytest.mark.parametrize(
        ("left_value", "right_value"),
        [(1, 2), (True, False), ("left", "right")],
    )
    def test_reserved_geometry_attribute_dtypes_match_geopandas(
        self, left_value, right_value
    ):
        left_local = gpd.GeoDataFrame(
            {
                "geometry": [left_value],
                "geom": [box(0, 0, 2, 2)],
            },
            geometry="geom",
            crs=4326,
        )
        right_local = gpd.GeoDataFrame(
            {
                "geometry": [right_value],
                "shape": [box(1, 1, 3, 3)],
            },
            geometry="shape",
            crs=4326,
        )

        result = GeoDataFrame(left_local).overlay(
            GeoDataFrame(right_local), how="identity", keep_geom_type=False
        )
        expected = gpd.overlay(
            left_local, right_local, how="identity", keep_geom_type=False
        )

        result_local = self._assert_overlay_equal(result, expected)
        assert result_local["geometry_1"].notna().sum() == 1
        assert result_local["geometry_2"].notna().sum() == 1

    def test_errors_for_mixed_multiindex_and_suffix_collision(self):
        polygons = GeoDataFrame({"geometry": [box(0, 0, 1, 1)], "value": [1]})
        mixed = GeoDataFrame({"geometry": [Point(0, 0), LineString([(0, 0), (1, 1)])]})
        with pytest.raises(NotImplementedError, match="mixed geometry types"):
            mixed.overlay(polygons)

        multi_columns = gpd.GeoDataFrame(
            [[1, box(0, 0, 1, 1)]],
            columns=pd.MultiIndex.from_tuples([("data", "value"), ("geo", "shape")]),
            geometry=("geo", "shape"),
        )
        with pytest.raises(NotImplementedError, match="MultiIndex columns"):
            GeoDataFrame(multi_columns).overlay(polygons)

        colliding_left = GeoDataFrame(
            {
                "name": [1],
                "name_1": [2],
                "geometry": [box(0, 0, 1, 1)],
            }
        )
        colliding_right = GeoDataFrame({"name": [3], "geometry": [box(0, 0, 1, 1)]})
        with pytest.raises(ValueError, match="duplicate output column"):
            colliding_left.overlay(colliding_right)
        # Right attributes do not participate in a standalone difference.
        assert len(colliding_left.overlay(colliding_right, how="difference")) == 0

    def test_intersection_plan_uses_native_spatial_join(self):
        left = GeoDataFrame(
            gpd.GeoDataFrame({"geometry": [box(0, 0, 2, 2)], "value": [1]}, crs=4326)
        )
        right = GeoDataFrame(
            gpd.GeoDataFrame({"geometry": [box(1, 1, 3, 3)], "value": [2]}, crs=4326)
        )

        result = left.overlay(right, keep_geom_type=False)
        plan = (
            result._internal.spark_frame._jdf.queryExecution().executedPlan().toString()
        )

        assert "RangeJoin" in plan or "BroadcastIndexJoin" in plan
        assert "CartesianProduct" not in plan
        assert "BatchEvalPython" not in plan
        assert "ArrowEvalPython" not in plan
