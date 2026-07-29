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
import pyspark.pandas as ps
import pytest
from geopandas.testing import assert_geoseries_equal

import sedona.spark.geopandas as sgpd
from sedona.spark.geopandas import GeoSeries
from sedona.spark.sql import st_functions as stf
from tests.geopandas.test_geopandas_base import TestGeopandasBase


def _assert_geoseries_equal_with_nan_xy(actual, expected, nan_xy):
    """Compare NaN x/y coordinates without relying on undefined GEOS equality."""
    assert actual.crs == expected.crs
    nan_xy = np.asarray(nan_xy, dtype=bool)
    nan_positions = np.flatnonzero(nan_xy)
    regular_positions = np.flatnonzero(~nan_xy)
    pd.testing.assert_series_equal(
        actual.to_wkt().iloc[nan_positions],
        expected.to_wkt().iloc[nan_positions],
    )
    if regular_positions.size:
        assert_geoseries_equal(
            actual.iloc[regular_positions],
            expected.iloc[regular_positions],
        )


class TestPointsFromXY(TestGeopandasBase):
    def test_points_from_xy_local_matches_geopandas(self):
        index = pd.Index(["first", "first", "third"], name="feature")
        x = pd.Series([1, 2, -3.5], index=index, name="longitude")
        y = pd.Series([4, 5, 6.5], index=index, name="latitude")

        result = sgpd.points_from_xy(x, y, crs="EPSG:4326")
        expected = gpd.GeoSeries(
            gpd.points_from_xy(x, y, crs="EPSG:4326"),
            index=index,
            crs="EPSG:4326",
        )

        assert result.name is None
        assert result.crs == "EPSG:4326"
        assert_geoseries_equal(result.to_geopandas(), expected)
        srids = result._internal.spark_frame.select(
            stf.ST_SRID(result.spark.column).alias("srid")
        ).collect()
        assert {row.srid for row in srids} == {4326}

    def test_points_from_xy_local_3d_broadcast_and_empty(self):
        result = sgpd.points_from_xy([1, 2], [3], [4, 5])
        # GeoPandas 0.13 does not broadcast its own inputs, so construct the
        # equivalent oracle from explicitly expanded coordinates.
        expected = gpd.GeoSeries(gpd.points_from_xy([1, 2], [3, 3], [4, 5]))
        assert_geoseries_equal(result.to_geopandas(), expected)

        scalar_result = sgpd.points_from_xy(1, [2, 3], 4)
        scalar_expected = gpd.GeoSeries(gpd.points_from_xy([1, 1], [2, 3], [4, 4]))
        assert_geoseries_equal(scalar_result.to_geopandas(), scalar_expected)

        empty = sgpd.points_from_xy([], [], crs="EPSG:3857")
        assert len(empty) == 0
        assert empty.crs == "EPSG:3857"

    def test_points_from_xy_distributed_preserves_multiindex_and_plan(self):
        index = pd.MultiIndex.from_tuples(
            [("a", 1), ("a", 1), ("b", 2)],
            names=["group", "position"],
        )
        pdf = pd.DataFrame(
            {
                "x": [1.0, np.nan, 3.0],
                "y": [4.0, 5.0, 6.0],
                "z": [7.0, 8.0, np.nan],
            },
            index=index,
        )
        psdf = ps.from_pandas(pdf)

        result = sgpd.points_from_xy(
            psdf["x"],
            psdf["y"],
            psdf["z"],
            crs="EPSG:4979",
        )
        expected = gpd.GeoSeries(
            gpd.points_from_xy(pdf["x"], pdf["y"], pdf["z"], crs="EPSG:4979"),
            index=index,
            crs="EPSG:4979",
        )

        assert result.crs == "EPSG:4979"
        nan_xy = pdf[["x", "y"]].isna().any(axis=1).to_numpy()
        _assert_geoseries_equal_with_nan_xy(
            result.to_geopandas(),
            expected,
            nan_xy,
        )
        srids = result._internal.spark_frame.select(
            stf.ST_SRID(result.spark.column).alias("srid")
        ).collect()
        assert {row.srid for row in srids} == {4979}

        spark_frame = result._internal.spark_frame
        if hasattr(spark_frame, "_jdf"):
            plan = spark_frame._jdf.queryExecution().executedPlan().toString()
            assert "BatchEvalPython" not in plan
            assert "ArrowEvalPython" not in plan
            assert "PythonUDF" not in plan

        scalar_result = sgpd.points_from_xy(psdf["x"], 10, crs="EPSG:4326")
        scalar_expected = gpd.GeoSeries(
            gpd.points_from_xy(pdf["x"], [10] * len(pdf), crs="EPSG:4326"),
            index=index,
            crs="EPSG:4326",
        )
        _assert_geoseries_equal_with_nan_xy(
            scalar_result.to_geopandas(),
            scalar_expected,
            pdf["x"].isna().to_numpy(),
        )

    def test_points_from_xy_preserves_pending_expressions(self):
        frame = ps.DataFrame(
            {
                "x": [1.0, 2.0],
                "y": [3.0, 4.0],
                "value": [10, 11],
            }
        )
        frame["computed_index"] = frame["value"] + 100
        frame = frame.set_index("computed_index")

        result = sgpd.points_from_xy(frame["x"] * 2, frame["y"] + 0.5)
        actual = result.to_geopandas()

        assert actual.index.equals(pd.Index([110, 111], name="computed_index"))
        assert actual.to_wkt().tolist() == [
            "POINT (2 3.5)",
            "POINT (4 4.5)",
        ]

    def test_points_from_xy_null_is_nan_coordinate_not_missing_geometry(self):
        psdf = self.spark.createDataFrame(
            [(0, None, 2.0), (1, 3.0, None), (2, None, None)],
            "id long, x double, y double",
        ).pandas_api(index_col="id")

        result = sgpd.points_from_xy(psdf["x"], psdf["y"])

        assert result.isna().to_pandas().tolist() == [False, False, False]
        expected = gpd.GeoSeries(
            gpd.points_from_xy([None, 3.0, None], [2.0, None, None]),
            index=pd.Index([0, 1, 2], name="id"),
        )
        _assert_geoseries_equal_with_nan_xy(
            result.to_geopandas(),
            expected,
            np.ones(len(expected), dtype=bool),
        )

    def test_geoseries_from_xy_delegates_to_distributed_constructor(self):
        result = GeoSeries.from_xy(
            [1, 2],
            [3, 4],
            index=pd.Index([10, 20], name="id"),
            crs="EPSG:4326",
            name="location",
        )
        expected = gpd.GeoSeries(
            gpd.points_from_xy([1, 2], [3, 4], crs="EPSG:4326"),
            index=pd.Index([10, 20], name="id"),
            crs="EPSG:4326",
            name="location",
        )

        assert_geoseries_equal(result.to_geopandas(), expected)

    def test_points_from_xy_validates_unsupported_inputs(self):
        with pytest.raises(TypeError, match="at least one coordinate"):
            sgpd.points_from_xy(1, 2)
        with pytest.raises(ValueError, match="broadcast-compatible"):
            sgpd.points_from_xy([1, 2], [3, 4, 5])

        frame = ps.DataFrame({"x": [1, 2], "y": [3, 4]})
        with pytest.raises(TypeError, match="numeric scalar"):
            sgpd.points_from_xy(frame["x"], [3, 4])
        string_frame = ps.DataFrame({"x": [1, 2], "y": ["a", "b"]})
        with pytest.raises(
            TypeError, match="y must be a numeric pandas-on-Spark Series"
        ):
            sgpd.points_from_xy(string_frame["x"], string_frame["y"])
        with pytest.raises(
            TypeError, match="x must be a numeric pandas-on-Spark Series"
        ):
            sgpd.points_from_xy(string_frame["y"], string_frame["x"])

        other_frame = ps.DataFrame({"y": [3, 4]})
        with pytest.raises(ValueError, match="same distributed frame"):
            sgpd.points_from_xy(frame["x"], other_frame["y"])
        with pytest.raises(TypeError, match="index cannot be supplied"):
            GeoSeries.from_xy(frame["x"], frame["y"], index=[0, 1])
