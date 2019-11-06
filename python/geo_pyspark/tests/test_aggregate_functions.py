from unittest import TestCase

from pyspark.sql import SparkSession
from shapely.geometry import Polygon

from geo_pyspark.data import csv_point_input_location, union_polygon_input_location
from geo_pyspark.register import GeoSparkRegistrator, upload_jars

upload_jars()


spark = SparkSession.builder. \
    getOrCreate()

GeoSparkRegistrator.registerAll(spark)


class TestConstructors(TestCase):

    def test_st_envelope_aggr(self):
        point_csv_df = spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(csv_point_input_location)

        point_csv_df.createOrReplaceTempView("pointtable")
        point_df = spark.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        boundary = spark.sql("select ST_Envelope_Aggr(pointdf.arealandmark) from pointdf")

        coordinates = [
            (1.1, 101.1),
            (1.1, 1100.1),
            (1000.1, 1100.1),
            (1000.1, 101.1),
            (1.1, 101.1)
        ]

        polygon = Polygon(coordinates)

        self.assertEqual(boundary.take(1)[0][0], polygon)

    def test_st_union_aggr(self):
        polygon_csv_df = spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(union_polygon_input_location)

        polygon_csv_df.createOrReplaceTempView("polygontable")
        polygon_csv_df.show()
        polygon_df = spark.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        union = spark.sql("select ST_Union_Aggr(polygondf.polygonshape) from polygondf")

        self.assertEqual(union.take(1)[0][0].area, 10100)

