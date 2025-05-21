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


from sedona.spark.sql.types import GeometryType
from sedona.spark.sql.functions import sedona_vectorized_udf, SedonaUDFType
from tests import chicago_crimes_input_location
from tests.test_base import TestBase
import pyspark.sql.functions as f
import shapely.geometry.base as b
import geopandas as gpd
import pytest
import pyspark
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType, FloatType
from shapely.geometry import Point
from shapely.wkt import loads


def non_vectorized_buffer_udf(geom: b.BaseGeometry) -> b.BaseGeometry:
    return geom.buffer(0.1)


@sedona_vectorized_udf(return_type=GeometryType())
def vectorized_buffer_udf(geom: b.BaseGeometry) -> b.BaseGeometry:
    return geom.buffer(0.1)


@sedona_vectorized_udf(return_type=FloatType())
def vectorized_geom_to_numeric_udf(geom: b.BaseGeometry) -> float:
    return geom.area


@sedona_vectorized_udf(return_type=FloatType())
def vectorized_geom_to_numeric_udf_child_geom(geom: Point) -> float:
    return geom.x


@sedona_vectorized_udf(return_type=GeometryType())
def vectorized_numeric_to_geom(x: float) -> b.BaseGeometry:
    return Point(x, x)


@sedona_vectorized_udf(udf_type=SedonaUDFType.GEO_SERIES, return_type=FloatType())
def vectorized_series_to_numeric_udf(series: gpd.GeoSeries) -> pd.Series:
    buffered = series.x

    return buffered


@sedona_vectorized_udf(udf_type=SedonaUDFType.GEO_SERIES, return_type=GeometryType())
def vectorized_series_string_to_geom(x: pd.Series) -> b.BaseGeometry:
    return x.apply(lambda x: loads(str(x)))


@sedona_vectorized_udf(udf_type=SedonaUDFType.GEO_SERIES, return_type=GeometryType())
def vectorized_series_string_to_geom_2(x: pd.Series):
    return x.apply(lambda x: loads(str(x)))


@sedona_vectorized_udf(udf_type=SedonaUDFType.GEO_SERIES, return_type=GeometryType())
def vectorized_series_buffer_udf(series: gpd.GeoSeries) -> gpd.GeoSeries:
    buffered = series.buffer(0.1)

    return buffered


@pandas_udf(IntegerType())
def squared_udf(s: pd.Series) -> pd.Series:
    return s**2  # Perform vectorized operation


buffer_distanced_udf = f.udf(non_vectorized_buffer_udf, GeometryType())


class TestSedonaArrowUDF(TestBase):

    def get_area(self, df, udf_fn):
        return (
            df.select(udf_fn(f.col("geom")).alias("buffer"))
            .selectExpr("SUM(ST_Area(buffer))")
            .collect()[0][0]
        )

    @pytest.mark.skipif(
        pyspark.__version__ < "3.5", reason="requires Spark 3.5 or higher"
    )
    def test_pandas_arrow_udf(self):
        df = (
            self.spark.read.option("header", "true")
            .format("csv")
            .load(chicago_crimes_input_location)
            .selectExpr("ST_Point(x, y) AS geom")
        )

        area1 = self.get_area(df, vectorized_buffer_udf)
        assert area1 > 478

    @pytest.mark.skipif(
        pyspark.__version__ < "3.5", reason="requires Spark 3.5 or higher"
    )
    def test_pandas_udf_shapely_geometry_and_numeric(self):
        df = (
            self.spark.read.option("header", "true")
            .format("csv")
            .load(chicago_crimes_input_location)
            .selectExpr("ST_Point(x, y) AS geom", "x")
            .select(
                vectorized_geom_to_numeric_udf(f.col("geom")).alias("area"),
                vectorized_geom_to_numeric_udf_child_geom(f.col("geom")).alias(
                    "x_coordinate"
                ),
                vectorized_numeric_to_geom(f.col("x").cast("float")).alias(
                    "geom_second"
                ),
            )
        )

        assert df.select(f.sum("area")).collect()[0][0] == 0.0
        assert -1339276 > df.select(f.sum("x_coordinate")).collect()[0][0] > -1339277
        assert (
            -1339276
            > df.selectExpr("ST_X(geom_second) AS x_coordinate")
            .select(f.sum("x_coordinate"))
            .collect()[0][0]
            > -1339277
        )

    @pytest.mark.skipif(
        pyspark.__version__ < "3.5", reason="requires Spark 3.5 or higher"
    )
    def test_pandas_udf_geoseries_geometry_and_numeric(self):
        df = (
            self.spark.read.option("header", "true")
            .format("csv")
            .load(chicago_crimes_input_location)
            .selectExpr(
                "ST_Point(x, y) AS geom",
                "CONCAT('POINT(', x, ' ', y, ')') AS wkt",
            )
            .select(
                vectorized_series_to_numeric_udf(f.col("geom")).alias("x_coordinate"),
                vectorized_series_string_to_geom(f.col("wkt")).alias("geom"),
                vectorized_series_string_to_geom_2(f.col("wkt")).alias("geom_2"),
            )
        )

        assert -1339276 > df.select(f.sum("x_coordinate")).collect()[0][0] > -1339277
        assert (
            -1339276
            > df.selectExpr("ST_X(geom) AS x_coordinate")
            .select(f.sum("x_coordinate"))
            .collect()[0][0]
            > -1339277
        )
        assert (
            -1339276
            > df.selectExpr("ST_X(geom_2) AS x_coordinate")
            .select(f.sum("x_coordinate"))
            .collect()[0][0]
            > -1339277
        )

    @pytest.mark.skipif(
        pyspark.__version__ < "3.5", reason="requires Spark 3.5 or higher"
    )
    def test_pandas_udf_numeric_to_geometry(self):
        df = (
            self.spark.read.option("header", "true")
            .format("csv")
            .load(chicago_crimes_input_location)
            .selectExpr("ST_Point(y, x) AS geom")
        )

        area1 = self.get_area(df, vectorized_buffer_udf)
        assert area1 > 478

    @pytest.mark.skipif(
        pyspark.__version__ < "3.5", reason="requires Spark 3.5 or higher"
    )
    def test_pandas_udf_numeric_and_numeric_to_geometry(self):
        df = (
            self.spark.read.option("header", "true")
            .format("csv")
            .load(chicago_crimes_input_location)
            .selectExpr("ST_Point(y, x) AS geom")
        )

        area1 = self.get_area(df, vectorized_buffer_udf)
        assert area1 > 478

    @pytest.mark.skipif(
        pyspark.__version__ < "3.5", reason="requires Spark 3.5 or higher"
    )
    def test_geo_series_udf(self):
        df = (
            self.spark.read.option("header", "true")
            .format("csv")
            .load(chicago_crimes_input_location)
            .selectExpr("ST_Point(y, x) AS geom")
        )

        area = self.get_area(df, vectorized_series_buffer_udf)

        assert area > 478

    def test_pandas_arrow_udf_compatibility(self):
        df = (
            self.spark.read.option("header", "true")
            .format("csv")
            .load(chicago_crimes_input_location)
            .selectExpr("CAST(x AS INT) AS x")
        )

        sum_value = df.select(f.sum(squared_udf(f.col("x")))).collect()[0][0]
        assert sum_value == 115578630
