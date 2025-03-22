from sedona.sql.types import GeometryType
from sedona.sql.functions import sedona_vectorized_udf, SedonaUDFType
from tests import chicago_crimes_input_location
from tests.test_base import TestBase
import pyspark.sql.functions as f
import shapely.geometry.base as b
from time import time
import geopandas as gpd
import pytest
import pyspark
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType


def non_vectorized_buffer_udf(geom: b.BaseGeometry) -> b.BaseGeometry:
    return geom.buffer(0.1)


@sedona_vectorized_udf()
def vectorized_buffer(geom: b.BaseGeometry) -> b.BaseGeometry:
    return geom.buffer(0.1)


@sedona_vectorized_udf(udf_type=SedonaUDFType.GEO_SERIES)
def vectorized_geo_series_buffer(series: gpd.GeoSeries) -> gpd.GeoSeries:
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
            .selectExpr("ST_Point(y, x) AS geom")
        )

        vectorized_times = []
        non_vectorized_times = []

        for i in range(5):
            start = time()
            area1 = self.get_area(df, vectorized_buffer)

            assert area1 > 478

            vectorized_times.append(time() - start)

            area2 = self.get_area(df, buffer_distanced_udf)

            assert area2 > 478

            non_vectorized_times.append(time() - start)

        for v, nv in zip(vectorized_times, non_vectorized_times):
            assert v < nv, "Vectorized UDF is slower than non-vectorized UDF"

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

        area = self.get_area(df, vectorized_buffer)

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
