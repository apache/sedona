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

from typing import Union
from tests.test_base import TestBase
from sedona.spark.geopandas import GeoDataFrame, GeoSeries
import pyspark.sql
import geopandas as gpd
import pandas as pd
import pyspark.pandas as ps
from pandas.testing import assert_series_equal
from contextlib import contextmanager
from shapely.geometry import GeometryCollection
from shapely.geometry.base import BaseGeometry
from pandas.testing import assert_index_equal


class TestGeopandasBase(TestBase):
    # -----------------------------------------------------------------------------
    # # Utils
    # -----------------------------------------------------------------------------
    def setup_method(self):
        # We enable this option by default for external users, but we disable it for development testing.
        # This is useful to catch inefficiencies in the code while developing this package.
        ps.set_option("compute.ops_on_diff_frames", False)

    @classmethod
    def check_sgpd_equals_spark_df(
        cls, actual: GeoSeries, expected: pyspark.sql.DataFrame
    ):
        assert isinstance(actual, GeoSeries)
        assert isinstance(expected, pyspark.sql.DataFrame)
        expected = expected.selectExpr("ST_AsText(expected) as expected")
        sgpd_result = actual.to_geopandas()
        expected = expected.toPandas()["expected"]
        for a, e in zip(sgpd_result, expected):
            cls.assert_geometry_almost_equal(a, e)

    # TODO chore: rename to check_sgpd_series_equals_gpd_series and change the names in the geoseries tests
    @classmethod
    def check_sgpd_equals_gpd(
        cls, actual: GeoSeries, expected: gpd.GeoSeries, tolerance: float = 1e-2
    ):
        assert isinstance(actual, GeoSeries)
        assert isinstance(expected, gpd.GeoSeries)
        assert actual.name == expected.name, "results are of different names"
        sgpd_result = actual.to_geopandas()
        assert len(sgpd_result) == len(expected), "results are of different lengths"
        for a, e in zip(sgpd_result, expected):
            if a is None or e is None:
                assert a is None and e is None
                continue
            # Sometimes sedona and geopandas both return empty geometries but of different types (e.g Point and Polygon)
            elif a.is_empty and e.is_empty:
                continue
            cls.assert_geometry_almost_equal(a, e, tolerance)

        assert_index_equal(actual.index.to_pandas(), expected.index)

    @classmethod
    def check_sgpd_df_equals_gpd_df(
        cls, actual: GeoDataFrame, expected: gpd.GeoDataFrame
    ):
        assert isinstance(actual, GeoDataFrame), "result is not a sgpd.GeoDataFrame"
        assert isinstance(
            expected, gpd.GeoDataFrame
        ), "expected is not a gpd.GeoDataFrame"
        assert len(actual.columns) == len(expected.columns)
        for col_name in actual.keys():
            actual_series, expected_series = actual[col_name], expected[col_name]
            if isinstance(actual_series, GeoSeries):
                assert isinstance(
                    actual_series, GeoSeries
                ), f"result[{col_name}] series is not a sgpd.GeoSeries"
                # original geopandas does not guarantee a GeoSeries will be returned, so convert it here
                expected_series = gpd.GeoSeries(expected_series)
                cls.check_sgpd_equals_gpd(actual_series, expected_series)
            else:
                assert isinstance(
                    actual_series, ps.Series
                ), f"result[{col_name}] series is not a ps.Series"
                assert isinstance(
                    expected_series, pd.Series
                ), f"expected[{col_name}] series is not a pd.Series"
                cls.check_pd_series_equal(actual_series, expected_series)

    @classmethod
    def check_pd_series_equal(cls, actual: ps.Series, expected: pd.Series):
        assert isinstance(actual, ps.Series), "result series is not a ps.Series"
        assert isinstance(expected, pd.Series), "expected series is not a pd.Series"
        assert_series_equal(actual.to_pandas(), expected)

    @classmethod
    def check_index_equal(
        cls, actual: Union[ps.DataFrame, ps.Series], expected: ps.Index
    ):
        assert_index_equal(actual.index, expected)

    @classmethod
    def contains_any_geom_collection(cls, geoms) -> bool:
        return any(isinstance(g, GeometryCollection) for g in geoms)

    def contains_any_geom_collection(self, geoms1, geoms2) -> bool:
        return any(isinstance(g, GeometryCollection) for g in geoms1) or any(
            isinstance(g, GeometryCollection) for g in geoms2
        )

    @classmethod
    def check_geom_equals(cls, actual: BaseGeometry, expected: BaseGeometry):
        assert isinstance(actual, BaseGeometry)
        assert isinstance(expected, BaseGeometry)
        assert actual.equals(expected)
