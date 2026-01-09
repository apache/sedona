from sedona.spark.sql.functions import sedona_db_vectorized_udf
from tests.test_base import TestBase
import pyarrow as pa
import shapely
from sedona.sql import GeometryType
from pyspark.sql.functions import expr, lit
from pyspark.sql.types import DoubleType, IntegerType
from sedona.spark.sql import ST_X


class TestSedonaDBArrowFunction(TestBase):
    def test_vectorized_udf(self):
        @sedona_db_vectorized_udf(return_type=GeometryType(), input_types=[GeometryType(), IntegerType()])
        def my_own_function(geom, distance):
            geom_wkb = pa.array(geom.storage.to_array())
            distance = pa.array(distance.to_array())
            geom = shapely.from_wkb(geom_wkb)

            result_shapely = shapely.centroid(geom)

            return pa.array(shapely.to_wkb(result_shapely))

        df = self.spark.createDataFrame(
            [
                (1, "POINT (1 1)"),
                (2, "POINT (2 2)"),
                (3, "POINT (3 3)"),
            ],
            ["id", "wkt"],
        ).withColumn("wkt", expr("ST_GeomFromWKT(wkt)"))

        df.select(ST_X(my_own_function(df.wkt, lit(100)).alias("geom"))).show()

    def test_geometry_to_double(self):
        @sedona_db_vectorized_udf(return_type=DoubleType(), input_types=[GeometryType()])
        def geometry_to_non_geometry_udf(geom):
            geom_wkb = pa.array(geom.storage.to_array())
            geom = shapely.from_wkb(geom_wkb)

            result_shapely = shapely.get_x(shapely.centroid(geom))

            return pa.array(result_shapely, pa.float64())

        df = self.spark.createDataFrame(
            [(1, "POINT (1 1)"), (2, "POINT (2 2)"), (3, "POINT (3 3)")],
            ["id", "wkt"],
        ).withColumn("wkt", expr("ST_GeomFromWKT(wkt)"))

        values = df.select(geometry_to_non_geometry_udf(df.wkt).alias("x_coord")) \
            .collect()

        values_list = [row["x_coord"] for row in values]

        assert values_list == [1.0, 2.0, 3.0]

    def test_geometry_to_int(self):
        @sedona_db_vectorized_udf(return_type=IntegerType(), input_types=[GeometryType()])
        def geometry_to_int(geom):
            geom_wkb = pa.array(geom.storage.to_array())
            geom = shapely.from_wkb(geom_wkb)

            result_shapely = shapely.get_num_points(geom)

            return pa.array(result_shapely, pa.int32())

        df = self.spark.createDataFrame(
            [(1, "POINT (1 1)"), (2, "POINT (2 2)"), (3, "POINT (3 3)")],
            ["id", "wkt"],
        ).withColumn("wkt", expr("ST_GeomFromWKT(wkt)"))

        values = df.select(geometry_to_int(df.wkt)) \
            .collect()

        values_list = [row[0] for row in values]

        assert values_list == [0, 0, 0]

    def test_geometry_crs_preservation(self):
        @sedona_db_vectorized_udf(return_type=GeometryType(), input_types=[GeometryType()])
        def return_same_geometry(geom):
            geom_wkb = pa.array(geom.storage.to_array())
            geom = shapely.from_wkb(geom_wkb)

            return pa.array(shapely.to_wkb(geom))

        df = self.spark.createDataFrame(
            [(1, "POINT (1 1)"), (2, "POINT (2 2)"), (3, "POINT (3 3)")],
            ["id", "wkt"],
        ).withColumn("wkt", expr("ST_SetSRID(ST_GeomFromWKT(wkt), 3857)"))

        result_df = df.select(return_same_geometry(df.wkt).alias("geom"))

        crs_list = result_df.selectExpr("ST_SRID(geom)").rdd.flatMap(lambda x: x).collect()

        assert crs_list == [3857, 3857, 3857]

    def test_geometry_to_geometry(self):
        @sedona_db_vectorized_udf(return_type=GeometryType(), input_types=[GeometryType()])
        def buffer_geometry(geom):
            geom_wkb = pa.array(geom.storage.to_array())
            geom = shapely.from_wkb(geom_wkb)

            result_shapely = shapely.buffer(geom, 10)

            return pa.array(shapely.to_wkb(result_shapely))

        df = self.spark.read.\
            format("geoparquet").\
            load("/Users/pawelkocinski/Desktop/projects/sedona-production/apache-sedona-book/data/warehouse/buildings_large_3")
        # 18 24
        # df.union(df).union(df).union(df).union(df).union(df).union(df).\
        #     write.format("geoparquet").mode("overwrite").save("/Users/pawelkocinski/Desktop/projects/sedona-production/apache-sedona-book/data/warehouse/buildings_large_3")

        values = df.select(buffer_geometry(df.geometry).alias("geometry")).\
            selectExpr("ST_Area(geometry) as area").\
            selectExpr("Sum(area) as total_area")

        values.show()

    def test_geometry_to_geometry_normal_udf(self):
        from pyspark.sql.functions import udf

        def create_buffer(geom):
            return geom.buffer(10)

        create_buffer_udf = udf(create_buffer, GeometryType())

        df = self.spark.read. \
            format("geoparquet"). \
            load("/Users/pawelkocinski/Desktop/projects/sedona-production/apache-sedona-book/data/warehouse/buildings_large_3")

        values = df.select(create_buffer_udf(df.geometry).alias("geometry")). \
            selectExpr("ST_Area(geometry) as area"). \
            selectExpr("Sum(area) as total_area")

        values.show()
