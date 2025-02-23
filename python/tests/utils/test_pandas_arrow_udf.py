from sedona.sql.types import GeometryType
from sedona.sql.udf import sedona_vectorized_udf
from tests import chicago_crimes_input_location
from tests.test_base import TestBase
import pyspark.sql.functions as f
import shapely.geometry.base as b
from time import time


def non_vectorized_buffer_udf(geom: b.BaseGeometry) -> b.BaseGeometry:
    return geom.buffer(0.001)


@sedona_vectorized_udf
def vectorized_buffer(geom: b.BaseGeometry) -> b.BaseGeometry:
    return geom.buffer(0.001)


buffer_distanced_udf = f.udf(non_vectorized_buffer_udf, GeometryType())


class TestSedonaArrowUDF(TestBase):

    def test_pandas_arrow_udf(self):
        df = (
            self.spark.read.option("header", "true")
            .format("csv")
            .load(chicago_crimes_input_location)
            .selectExpr("ST_Point(y, x) AS geom")
        )

        vectorized_times = []
        non_vectorized_times = []

        for i in range(10):
            start = time()
            df = df.withColumn("buffer", vectorized_buffer(f.col("geom")))
            df.count()
            vectorized_times.append(time() - start)

            df = df.withColumn("buffer", buffer_distanced_udf(f.col("geom")))
            df.count()
            non_vectorized_times.append(time() - start)

        for v, nv in zip(vectorized_times, non_vectorized_times):
            assert v < nv, "Vectorized UDF is slower than non-vectorized UDF"
