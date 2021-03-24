#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from pyspark import Row
from pyspark.sql.functions import broadcast, expr
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, DoubleType

from tests import csv_polygon_input_location, csv_point_input_location, overlap_polygon_input_location, \
    csv_point1_input_location, csv_point2_input_location, csv_polygon1_input_location, csv_polygon2_input_location, \
    csv_polygon1_random_input_location, csv_polygon2_random_input_location
from tests.test_base import TestBase


class TestPredicateJoin(TestBase):

    def test_st_contains_in_join(self):
        polygon_csv_df = self.spark.read.format("csv").\
                option("delimiter", ",").\
                option("header", "false").load(
            csv_polygon_input_location
        )
        polygon_csv_df.createOrReplaceTempView("polygontable")
        polygon_csv_df.show()

        polygon_df = self.spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        point_csv_df = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
            csv_point_input_location
        )
        point_csv_df.createOrReplaceTempView("pointtable")
        point_csv_df.show()

        point_df = self.spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        point_df.show()

        range_join_df = self.spark.sql(
            "select * from polygondf, pointdf where ST_Contains(polygondf.polygonshape,pointdf.pointshape) ")

        range_join_df.explain()
        range_join_df.show(3)
        assert range_join_df.count() == 1000

    def test_st_intersects_in_a_join(self):
        polygon_csv_df = self.spark.read.format("csv").option("delimiter", ",").option("header", "false").load(
            csv_polygon_input_location
        )
        polygon_csv_df.createOrReplaceTempView("polygontable")
        polygon_csv_df.show()

        polygon_df = self.spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        point_csv_df = self.spark.read.format("csv").option("delimiter", ",").option("header", "false").load(
            csv_point_input_location
        )
        point_csv_df.createOrReplaceTempView("pointtable")
        point_csv_df.show()

        point_df = self.spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        point_df.show()

        range_join_df = self.spark.sql(
            "select * from polygondf, pointdf where ST_Intersects(polygondf.polygonshape,pointdf.pointshape) ")

        range_join_df.explain()
        range_join_df.show(3)
        assert range_join_df.count() == 1000

    def test_st_touches_in_a_join(self):
        polygon_csv_df = self.spark.read.format("csv").option("delimiter", ",").option("header", "false").load(csv_polygon_input_location)
        polygon_csv_df.createOrReplaceTempView("polygontable")
        polygon_csv_df.show()
        polygon_df = self.spark.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        point_csv_df = self.spark.read.format("csv").option("delimiter", ",").option("header", "false").load(csv_point_input_location)
        point_csv_df.createOrReplaceTempView("pointtable")
        point_csv_df.show()
        point_df = self.spark.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        point_df.show()

        range_join_df = self.spark.sql("select * from polygondf, pointdf where ST_Touches(polygondf.polygonshape,pointdf.pointshape) ")

        range_join_df.explain()
        range_join_df.show(3)
        assert range_join_df.count() == 1000

    def test_st_within_in_a_join(self):
        polygon_csv_df = self.spark.read.format("csv").option("delimiter", ",").option("header", "false").load(
            csv_polygon_input_location)
        polygon_csv_df.createOrReplaceTempView("polygontable")
        polygon_csv_df.show()

        polygon_df = self.spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        point_csv_df = self.spark.read.format("csv").option("delimiter", ",").option("header", "false").load(
            csv_point_input_location)
        point_csv_df.createOrReplaceTempView("pointtable")
        point_csv_df.show()

        point_df = self.spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        point_df.show()

        range_join_df = self.spark.sql(
            "select * from polygondf, pointdf where ST_Within(pointdf.pointshape, polygondf.polygonshape) ")

        range_join_df.explain()
        range_join_df.show(3)
        assert range_join_df.count() == 1000

    def test_st_overlaps_in_a_join(self):
        polygon_csv_df = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
                csv_polygon_input_location
        )
        polygon_csv_df.createOrReplaceTempView("polygontable")

        polygon_df = self.spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")

        polygon_csv_overlap_df = self.spark.read.format("csv").option("delimiter", ",").option("header", "false").load(
            overlap_polygon_input_location)
        polygon_csv_overlap_df.createOrReplaceTempView("polygonoverlaptable")

        polygon_overlap_df = self.spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygonoverlaptable._c0 as Decimal(24,20)),cast(polygonoverlaptable._c1 as Decimal(24,20)), cast(polygonoverlaptable._c2 as Decimal(24,20)), cast(polygonoverlaptable._c3 as Decimal(24,20))) as polygonshape from polygonoverlaptable")
        polygon_overlap_df.createOrReplaceTempView("polygonodf")

        range_join_df = self.spark.sql(
            "select * from polygondf, polygonodf where ST_Overlaps(polygondf.polygonshape, polygonodf.polygonshape)")

        range_join_df.explain()
        range_join_df.show(3)
        assert range_join_df.count() == 57

    def test_st_crosses_in_a_join(self):
        polygon_csv_df = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
            csv_polygon_input_location
        )
        polygon_csv_df.createOrReplaceTempView("polygontable")
        polygon_csv_df.show()

        polygon_df = self.spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        point_csv_df = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
            csv_point_input_location
        )
        point_csv_df.createOrReplaceTempView("pointtable")
        point_csv_df.show()

        point_df = self.spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        point_df.show()

        range_join_df = self.spark.sql(
            "select * from polygondf, pointdf where ST_Crosses(pointdf.pointshape, polygondf.polygonshape) ")

        range_join_df.explain()
        range_join_df.show(3)
        assert range_join_df.count() == 1000

    def test_st_distance_radius_in_a_join(self):
        point_csv_df_1 = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
            csv_point_input_location
        )
        point_csv_df_1.createOrReplaceTempView("pointtable")
        point_csv_df_1.show()

        point_df_1 = self.spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape1 from pointtable")
        point_df_1.createOrReplaceTempView("pointdf1")
        point_df_1.show()

        point_csv_df_2 = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
            csv_point_input_location)
        point_csv_df_2.createOrReplaceTempView("pointtable")
        point_csv_df_2.show()

        point_df2 = self.spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape2 from pointtable")
        point_df2.createOrReplaceTempView("pointdf2")
        point_df2.show()

        distance_join_df = self.spark.sql(
            "select * from pointdf1, pointdf2 where ST_Distance(pointdf1.pointshape1,pointdf2.pointshape2) <= 2")
        distance_join_df.explain()
        distance_join_df.show(10)
        assert distance_join_df.count() == 2998

    def test_st_distance_less_radius_in_a_join(self):
        point_csv_df_1 = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(csv_point_input_location)

        point_csv_df_1.createOrReplaceTempView("pointtable")
        point_csv_df_1.show()

        point_df1 = self.spark.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape1 from pointtable")
        point_df1.createOrReplaceTempView("pointdf1")
        point_df1.show()

        point_csv_df2 = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(csv_point_input_location)
        point_csv_df2.createOrReplaceTempView("pointtable")
        point_csv_df2.show()
        point_df2 = self.spark.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape2 from pointtable")
        point_df2.createOrReplaceTempView("pointdf2")
        point_df2.show()

        distance_join_df = self.spark.sql("select * from pointdf1, pointdf2 where ST_Distance(pointdf1.pointshape1,pointdf2.pointshape2) < 2")
        distance_join_df.explain()
        distance_join_df.show(10)
        assert distance_join_df.count() == 2998

    def test_st_contains_in_a_range_and_join(self):
        polygon_csv_df = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(csv_polygon_input_location)

        polygon_csv_df.createOrReplaceTempView("polygontable")
        polygon_csv_df.show()
        polygon_df = self.spark.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        point_csv_df = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(csv_point_input_location)
        point_csv_df.createOrReplaceTempView("pointtable")
        point_csv_df.show()
        point_df = self.spark.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        point_df.show()

        range_join_df = self.spark.sql("select * from polygondf, pointdf where ST_Contains(polygondf.polygonshape,pointdf.pointshape) " +
        "and ST_Contains(ST_PolygonFromEnvelope(1.0,101.0,501.0,601.0), polygondf.polygonshape)")

        range_join_df.explain()
        range_join_df.show(3)
        assert range_join_df.count() == 500

    def test_super_small_data_join(self):
        raw_point_df = self.spark.createDataFrame(
            self.spark.sparkContext.parallelize(
                [Row(1, "40.0", "-120.0"), Row(2, "30.0", "-110.0"), Row(3, "20.0", "-100.0")]
            ),
            StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField("lat", StringType(), True),
                    StructField("lon", StringType(), True)
                ]
            )
        )

        raw_point_df.createOrReplaceTempView("rawPointDf")

        pointDF = self.spark.sql(
            "select id, ST_Point(cast(lat as Decimal(24,20)), cast(lon as Decimal(24,20))) AS latlon_point FROM rawPointDf")
        pointDF.createOrReplaceTempView("pointDf")
        pointDF.show(truncate=False)

        raw_polygon_df = self.spark.createDataFrame(
            self.spark.sparkContext.parallelize(
                [
                    Row("A", 25.0, -115.0, 35.0, -105.0), Row("B", 25.0, -135.0, 35.0, -125.0)
                ]),
            StructType(
                [
                    StructField("id", StringType(), True), StructField("latmin", DoubleType(), True),
                    StructField("lonmin", DoubleType(), True), StructField("latmax", DoubleType(), True),
                    StructField("lonmax", DoubleType(), True)
                ]
            ))
        raw_polygon_df.createOrReplaceTempView("rawPolygonDf")

        polygon_envelope_df = self.spark.sql("select id, ST_PolygonFromEnvelope(" +
                                             "cast(latmin as Decimal(24,20)), cast(lonmin as Decimal(24,20)), " +
                                             "cast(latmax as Decimal(24,20)), cast(lonmax as Decimal(24,20))) AS polygon FROM rawPolygonDf")
        polygon_envelope_df.createOrReplaceTempView("polygonDf")

        within_envelope_df = self.spark.sql(
            "select * FROM pointDf, polygonDf WHERE ST_Within(pointDf.latlon_point, polygonDf.polygon)")
        assert within_envelope_df.count() == 1

    def test_st_equals_in_a_join_for_st_point(self):

        point_csv_df_1 = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(csv_point1_input_location)

        point_csv_df_1.createOrReplaceTempView("pointtable1")
        point_csv_df_1.show()
        point_df1 = self.spark.sql("select ST_Point(cast(pointtable1._c0 as Decimal(24,20)),cast(pointtable1._c1 as Decimal(24,20)) ) as pointshape1 from pointtable1")
        point_df1.createOrReplaceTempView("pointdf1")
        point_df1.show()

        point_csv_df2 = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(csv_point2_input_location)
        point_csv_df2.createOrReplaceTempView("pointtable2")
        point_csv_df2.show()
        point_df2 = self.spark.sql("select ST_Point(cast(pointtable2._c0 as Decimal(24,20)),cast(pointtable2._c1 as Decimal(24,20))) as pointshape2 from pointtable2")
        point_df2.createOrReplaceTempView("pointdf2")
        point_df2.show()

        equal_join_df = self.spark.sql("select * from pointdf1, pointdf2 where ST_Equals(pointdf1.pointshape1,pointdf2.pointshape2) ")

        equal_join_df.explain()
        equal_join_df.show(3)
        assert equal_join_df.count() == 100, f"Expected 100 but got {equal_join_df.count()}"

    def test_st_equals_in_a_join_for_st_polygon(self):
        polygon_csv_df1 = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(csv_polygon1_input_location)

        polygon_csv_df1.createOrReplaceTempView("polygontable1")
        polygon_csv_df1.show()

        polygon_df1 = self.spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable1._c0 as Decimal(24,20)),cast(polygontable1._c1 as Decimal(24,20)), cast(polygontable1._c2 as Decimal(24,20)), cast(polygontable1._c3 as Decimal(24,20))) as polygonshape1 from polygontable1")
        polygon_df1.createOrReplaceTempView("polygondf1")
        polygon_df1.show()

        polygon_csv_df2 = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(csv_polygon2_input_location)

        polygon_csv_df2.createOrReplaceTempView("polygontable2")
        polygon_csv_df2.show()

        polygon_df2 = self.spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable2._c0 as Decimal(24,20)),cast(polygontable2._c1 as Decimal(24,20)), cast(polygontable2._c2 as Decimal(24,20)), cast(polygontable2._c3 as Decimal(24,20))) as polygonshape2 from polygontable2")
        polygon_df2.createOrReplaceTempView("polygondf2")
        polygon_df2.show()

        equal_join_df = self.spark.sql(
            "select * from polygondf1, polygondf2 where ST_Equals(polygondf1.polygonshape1,polygondf2.polygonshape2) ")

        equal_join_df.explain()
        equal_join_df.show(3)
        assert equal_join_df.count() == 100, f"Expected 100 but got {equal_join_df.count()}"

    def test_st_equals_in_a_join_for_st_polygon_random_shuffle(self):
        polygon_csv_df1 = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(csv_polygon1_random_input_location)
        polygon_csv_df1.createOrReplaceTempView("polygontable1")
        polygon_csv_df1.show()
        polygon_df1 = self.spark.sql("select ST_PolygonFromEnvelope(cast(polygontable1._c0 as Decimal(24,20)),cast(polygontable1._c1 as Decimal(24,20)), cast(polygontable1._c2 as Decimal(24,20)), cast(polygontable1._c3 as Decimal(24,20))) as polygonshape1 from polygontable1")
        polygon_df1.createOrReplaceTempView("polygondf1")
        polygon_df1.show()

        polygon_csv_df2 = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(csv_polygon2_random_input_location)

        polygon_csv_df2.createOrReplaceTempView("polygontable2")
        polygon_csv_df2.show()
        polygon_df2 = self.spark.sql("select ST_PolygonFromEnvelope(cast(polygontable2._c0 as Decimal(24,20)),cast(polygontable2._c1 as Decimal(24,20)), cast(polygontable2._c2 as Decimal(24,20)), cast(polygontable2._c3 as Decimal(24,20))) as polygonshape2 from polygontable2")
        polygon_df2.createOrReplaceTempView("polygondf2")
        polygon_df2.show()

        equal_join_df = self.spark.sql("select * from polygondf1, polygondf2 where ST_Equals(polygondf1.polygonshape1,polygondf2.polygonshape2) ")

        equal_join_df.explain()
        equal_join_df.show(3)
        assert equal_join_df.count() == 100, f"Expected 100 but got {equal_join_df.count()}"

    def test_st_equals_in_a_join_for_st_point_and_st_polygon(self):
        point_csv_df = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(csv_point1_input_location)

        point_csv_df.createOrReplaceTempView("pointtable")
        point_csv_df.show()
        point_df = self.spark.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20)) ) as pointshape from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        point_df.show()

        polygon_csv_df = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(csv_polygon1_input_location)

        polygon_csv_df.createOrReplaceTempView("polygontable")
        polygon_csv_df.show()
        polygon_df = self.spark.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        equal_join_df = self.spark.sql("select * from pointdf, polygondf where ST_Equals(pointdf.pointshape,polygondf.polygonshape) ")

        equal_join_df.explain()
        equal_join_df.show(3)
        assert equal_join_df.count() == 0, f"Expected 0 but got {equal_join_df.count()}"

    def test_st_contains_in_broadcast_join(self):
        polygon_csv_df = self.spark.read.format("csv").\
                option("delimiter", ",").\
                option("header", "false").load(
            csv_polygon_input_location
        )
        polygon_csv_df.createOrReplaceTempView("polygontable")
        polygon_csv_df.show()

        polygon_df = self.spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df = polygon_df.repartition(7)
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        point_csv_df = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
            csv_point_input_location
        )
        point_csv_df.createOrReplaceTempView("pointtable")
        point_csv_df.show()

        point_df = self.spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
        point_df = point_df.repartition(9)
        point_df.createOrReplaceTempView("pointdf")
        point_df.show()

        range_join_df = self.spark.sql(
            "select /*+ BROADCAST(polygondf) */ * from polygondf, pointdf where ST_Contains(polygondf.polygonshape,pointdf.pointshape) ")

        range_join_df.explain()
        range_join_df.show(3)
        assert range_join_df.rdd.getNumPartitions() == 9
        assert range_join_df.count() == 1000

        range_join_df = point_df.alias("pointdf").join(broadcast(polygon_df).alias("polygondf"), on=expr("ST_Contains(polygondf.polygonshape, pointdf.pointshape)"))

        range_join_df.explain()
        range_join_df.show(3)
        assert range_join_df.rdd.getNumPartitions() == 9
        assert range_join_df.count() == 1000

        
