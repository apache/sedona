from unittest import TestCase

from pyspark.sql import SparkSession

from geo_pyspark.data import csv_point_input_location, csv_point1_input_location, csv_polygon1_input_location
from geo_pyspark.register import GeoSparkRegistrator, upload_jars

upload_jars()

spark = SparkSession.builder. \
    getOrCreate()

GeoSparkRegistrator.registerAll(spark)


class TestPredicate(TestCase):

    def test_st_contains(self):
        point_csv_df = spark.read. \
            format("csv"). \
            option("delimiter", ","). \
            option("header", "false").load(
                csv_point_input_location
            )

        point_csv_df.createOrReplaceTempView("pointtable")
        point_df = spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
        point_df.createOrReplaceTempView("pointdf")

        result_df = spark.sql(
            "select * from pointdf where ST_Contains(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)")
        result_df.show()
        self.assertEqual(result_df.count(), 999)

    def test_st_intersects(self):
        point_csv_df = spark.read. \
            format("csv"). \
            option("delimiter", ","). \
            option("header", "false").load(
                csv_point_input_location
            )

        point_csv_df.createOrReplaceTempView("pointtable")

        point_df = spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        result_df = spark.sql(
            "select * from pointdf where ST_Intersects(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)")
        result_df.show()
        self.assertEqual(result_df.count(), 999)

    def test_st_within(self):
        point_csv_df = spark.read. \
            format("csv"). \
            option("delimiter", ","). \
            option("header", "false").load(
                csv_point_input_location
            )

        point_csv_df.createOrReplaceTempView("pointtable")

        point_df = spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        result_df = spark.sql(
            "select * from pointdf where ST_Within(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))")
        result_df.show()
        self.assertEqual(result_df.count(), 999)

    def test_st_equals_for_st_point(self):
        point_df_csv = spark.read.\
            format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
                csv_point1_input_location
            )

        point_df_csv.createOrReplaceTempView("pointtable")

        point_df = spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as point from pointtable")
        point_df.createOrReplaceTempView("pointdf")

        equal_df = spark.sql("select * from pointdf where ST_Equals(pointdf.point, ST_Point(100.1, 200.1)) ")
        equal_df.show()

        self.assertEqual(equal_df.count(), 5, f"Expected 5 value but got {equal_df.count()}")

    def test_st_equals_for_polygon(self):
        polygon_csv_df = spark.read.format("csv").\
                option("delimiter", ",").\
                option("header", "false").load(
                csv_polygon1_input_location
        )
        polygon_csv_df.createOrReplaceTempView("polygontable")

        polygon_df = spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        equal_df1 = spark.sql(
            "select * from polygonDf where ST_Equals(polygonDf.polygonshape, ST_PolygonFromEnvelope(100.01,200.01,100.5,200.5)) ")
        equal_df1.show()

        assert equal_df1.count() == 5, f"Expected 5 value but got ${equal_df1.count()}"

        equal_df_2 = spark.sql(
            "select * from polygonDf where ST_Equals(polygonDf.polygonshape, ST_PolygonFromEnvelope(100.5,200.5,100.01,200.01)) ")
        equal_df_2.show()

        self.assertEqual(equal_df_2.count(), 5, f"Expected 5 value but got {equal_df_2.count()}")

    def test_st_equals_for_st_point_and_st_polygon(self):
        polygon_csv_df = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(
            csv_polygon1_input_location)
        polygon_csv_df.createOrReplaceTempView("polygontable")

        polygon_df = spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()
        equal_df = spark.sql(
            "select * from polygonDf where ST_Equals(polygonDf.polygonshape, ST_Point(91.01,191.01)) ")
        equal_df.show()

        self.assertEqual(equal_df.count(), 0, f"Expected 0 value but got {equal_df.count()}")

    def test_st_equals_for_st_linestring_and_st_polygon(self):
        polygon_csv_df = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(
            csv_polygon1_input_location)
        polygon_csv_df.createOrReplaceTempView("polygontable")

        polygon_df = spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        string = "100.01,200.01,100.5,200.01,100.5,200.5,100.01,200.5,100.01,200.01"
        equal_df = spark.sql(f"select * from polygonDf where ST_Equals(polygonDf.polygonshape, ST_LineStringFromText(\'{string}\', \',\')) ")
        equal_df.show()

        self.assertEqual(equal_df.count(), 0, f"Expected 0 value but got {equal_df.count()}")

    def test_st_equals_for_st_polygon_from_envelope_and_st_polygon_from_text(self):
        polygon_csv_df = spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
            csv_polygon1_input_location
        )
        polygon_csv_df.createOrReplaceTempView("polygontable")

        polygon_df = spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        string = "100.01,200.01,100.5,200.01,100.5,200.5,100.01,200.5,100.01,200.01"

        equal_df = spark.sql(
        f"select * from polygonDf where ST_Equals(polygonDf.polygonshape, ST_PolygonFromText(\'{string}\', \',\')) ")
        equal_df.show()

        self.assertEqual(equal_df.count(), 5, f"Expected 5 value but got {equal_df.count()}")

    def test_st_crosses(self):
        crosses_test_table = spark.sql(
            "select ST_GeomFromWKT('POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))') as a,ST_GeomFromWKT('LINESTRING(1 5, 5 1)') as b")
        crosses_test_table.createOrReplaceTempView("crossesTesttable")
        crosses = spark.sql("select(ST_Crosses(a, b)) from crossesTesttable")

        not_crosses_test_table = spark.sql(
            "select ST_GeomFromWKT('POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))') as a,ST_GeomFromWKT('POLYGON((2 2, 5 2, 5 5, 2 5, 2 2))') as b")
        not_crosses_test_table.createOrReplaceTempView("notCrossesTesttable")
        not_crosses = spark.sql("select(ST_Crosses(a, b)) from notCrossesTesttable")

        self.assertEqual(crosses.take(1)[0][0], True)
        self.assertEqual(not_crosses.take(1)[0][0], False)

    def test_st_touches(self):
        point_csv_df = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(
            csv_point_input_location
        )
        point_csv_df.createOrReplaceTempView("pointtable")
        point_df = spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
        point_df.createOrReplaceTempView("pointdf")

        result_df = spark.sql(
            "select * from pointdf where ST_Touches(pointdf.arealandmark, ST_PolygonFromEnvelope(0.0,99.0,1.1,101.1))")
        result_df.show()
        self.assertEqual(result_df.count(), 1)

    def test_st_overlaps(self):
        test_table = spark.sql(
            "select ST_GeomFromWKT('POLYGON((2.5 2.5, 2.5 4.5, 4.5 4.5, 4.5 2.5, 2.5 2.5))') as a,ST_GeomFromWKT('POLYGON((4 4, 4 6, 6 6, 6 4, 4 4))') as b, ST_GeomFromWKT('POLYGON((5 5, 4 6, 6 6, 6 4, 5 5))') as c, ST_GeomFromWKT('POLYGON((5 5, 4 6, 6 6, 6 4, 5 5))') as d")
        test_table.createOrReplaceTempView("testtable")
        overlaps = spark.sql("select ST_Overlaps(a,b) from testtable")
        not_overlaps = spark.sql("select ST_Overlaps(c,d) from testtable")
        self.assertEqual(overlaps.take(1)[0][0], True)
        self.assertEqual(not_overlaps.take(1)[0][0], False)
