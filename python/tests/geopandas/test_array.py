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
from tests.geopandas.test_geopandas_base import TestGeopandasBase


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

    def test_points_from_xy_local_3d_broadcast_and_empty(self):
        result = sgpd.points_from_xy([1, 2], [3], [4, 5])
        expected = gpd.GeoSeries(gpd.points_from_xy([1, 2], [3], [4, 5]))
        assert_geoseries_equal(result.to_geopandas(), expected)

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
        assert_geoseries_equal(result.to_geopandas(), expected)

        spark_frame = result._internal.spark_frame
        if hasattr(spark_frame, "_jdf"):
            plan = spark_frame._jdf.queryExecution().executedPlan().toString()
            assert "BatchEvalPython" not in plan
            assert "ArrowEvalPython" not in plan
            assert "PythonUDF" not in plan

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
        # GEOS equality is undefined when one coordinate is NaN, so compare
        # their serializations after confirming these are not missing rows.
        assert result.to_geopandas().to_wkt().tolist() == expected.to_wkt().tolist()

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
        with pytest.raises(TypeError, match="iterable of numeric values"):
            sgpd.points_from_xy(1, [2])
        with pytest.raises(ValueError, match="broadcast-compatible"):
            sgpd.points_from_xy([1, 2], [3, 4, 5])

        frame = ps.DataFrame({"x": [1, 2], "y": [3, 4]})
        with pytest.raises(TypeError, match="must all be pandas-on-Spark Series"):
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
