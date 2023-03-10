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

import logging

import pyspark
import pytest
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.functions import col

from sedona import version
from sedona.core.SpatialRDD import PolygonRDD, CircleRDD
from sedona.core.enums import FileDataSplitter, GridType, IndexType
from sedona.core.formatMapper.shapefileParser.shape_file_reader import ShapefileReader
from sedona.core.geom.envelope import Envelope
from sedona.core.jvm.config import is_greater_or_equal_version
from sedona.core.spatialOperator import JoinQuery
from sedona.utils.adapter import Adapter
from tests import geojson_input_location, shape_file_with_missing_trailing_input_location, \
    geojson_id_input_location
from tests import shape_file_input_location, area_lm_point_input_location
from tests import mixed_wkt_geometry_input_location
from tests.test_base import TestBase


class TestAdapter(TestBase):

    def test_read_csv_point_into_spatial_rdd(self):
        df = self.spark.read.\
            format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(area_lm_point_input_location)

        df.show()
        df.createOrReplaceTempView("inputtable")

        spatial_df = self.spark.sql("select ST_PointFromText(inputtable._c0,\",\") as arealandmark from inputtable")
        spatial_df.show()
        spatial_df.printSchema()

        spatial_rdd = Adapter.toSpatialRdd(spatial_df, "arealandmark")
        spatial_rdd.analyze()
        Adapter.toDf(spatial_rdd, self.spark).show()

    def test_read_csv_point_into_spatial_rdd_by_passing_coordinates(self):
        df = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(area_lm_point_input_location)

        df.show()
        df.createOrReplaceTempView("inputtable")

        spatial_df = self.spark.sql(
            "select ST_Point(cast(inputtable._c0 as Decimal(24,20)),cast(inputtable._c1 as Decimal(24,20))) as arealandmark from inputtable"
        )

        spatial_df.show()
        spatial_df.printSchema()

    def test_read_csv_point_into_spatial_rdd_with_unique_id_by_passing_coordinates(self):
        df = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(area_lm_point_input_location)

        df.show()
        df.createOrReplaceTempView("inputtable")

        spatial_df = self.spark.sql(
            "select ST_Point(cast(inputtable._c0 as Decimal(24,20)),cast(inputtable._c1 as Decimal(24,20))) as arealandmark from inputtable")

        spatial_df.show()
        spatial_df.printSchema()

    def test_read_mixed_wkt_geometries_into_spatial_rdd(self):
        df = self.spark.read.format("csv").\
            option("delimiter", "\t").\
            option("header", "false").load(mixed_wkt_geometry_input_location)

        df.show()
        df.createOrReplaceTempView("inputtable")
        spatial_df = self.spark.sql("select ST_GeomFromWKT(inputtable._c0) as usacounty from inputtable")
        spatial_df.show()
        spatial_df.printSchema()
        spatial_rdd = Adapter.toSpatialRdd(spatial_df, "usacounty")
        spatial_rdd.analyze()
        Adapter.toDf(spatial_rdd, self.spark).show()
        assert (Adapter.toDf(spatial_rdd, self.spark).columns.__len__() == 1)
        Adapter.toDf(spatial_rdd, self.spark).show()

    def test_read_mixed_wkt_geometries_into_spatial_rdd_with_unique_id(self):
        df = self.spark.read.format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(mixed_wkt_geometry_input_location)

        df.show()
        df.createOrReplaceTempView("inputtable")

        spatial_df = self.spark.sql(
            "select ST_GeomFromWKT(inputtable._c0) as usacounty, inputtable._c3, inputtable._c5 from inputtable")
        spatial_df.show()
        spatial_df.printSchema()

        spatial_rdd = Adapter.toSpatialRdd(spatial_df, "usacounty")
        spatial_rdd.analyze()
        assert (Adapter.toDf(spatial_rdd, self.spark).columns.__len__() == 3)
        Adapter.toDf(spatial_rdd, self.spark).show()

    def test_read_shapefile_to_dataframe(self):
        spatial_rdd = ShapefileReader.readToGeometryRDD(
            self.spark.sparkContext, shape_file_input_location)
        spatial_rdd.analyze()
        logging.info(spatial_rdd.fieldNames)
        df = Adapter.toDf(spatial_rdd, self.spark)
        df.show()

    def test_read_shapefile_with_missing_to_dataframe(self):
        spatial_rdd = ShapefileReader.\
            readToGeometryRDD(self.spark.sparkContext, shape_file_with_missing_trailing_input_location)

        spatial_rdd.analyze()
        logging.info(spatial_rdd.fieldNames)

        df = Adapter.toDf(spatial_rdd, self.spark)
        df.show()

    def test_geojson_to_dataframe(self):
        spatial_rdd = PolygonRDD(
            self.spark.sparkContext, geojson_input_location, FileDataSplitter.GEOJSON, True
        )

        spatial_rdd.analyze()
        Adapter.toDf(spatial_rdd, self.spark).show()
        df = Adapter.toDf(spatial_rdd, self.spark)

        assert (df.columns[1] == "STATEFP")

    def test_convert_spatial_join_result_to_dataframe(self):
        polygon_wkt_df = self.spark.read.format("csv").option("delimiter", "\t").option("header", "false").load(
            mixed_wkt_geometry_input_location)
        polygon_wkt_df.createOrReplaceTempView("polygontable")

        polygon_df = self.spark.sql(
            "select ST_GeomFromWKT(polygontable._c0) as usacounty from polygontable")
        polygon_rdd = Adapter.toSpatialRdd(polygon_df, "usacounty")

        polygon_rdd.analyze()

        point_csv_df = self.spark.read.format("csv").option("delimiter", ",").option("header", "false").load(
            area_lm_point_input_location)
        point_csv_df.createOrReplaceTempView("pointtable")

        point_df = self.spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")

        point_rdd = Adapter.toSpatialRdd(point_df, "arealandmark")
        point_rdd.analyze()

        point_rdd.spatialPartitioning(GridType.QUADTREE)
        polygon_rdd.spatialPartitioning(point_rdd.getPartitioner())

        point_rdd.buildIndex(IndexType.QUADTREE, True)

        join_result_point_rdd = JoinQuery.\
            SpatialJoinQueryFlat(point_rdd, polygon_rdd, True, True)

        join_result_df = Adapter.toDf(join_result_point_rdd, self.spark)
        join_result_df.show()

        join_result_df2 = Adapter.toDf(join_result_point_rdd, ["abc", "def"], list(), self.spark)
        join_result_df2.show()

    def test_distance_join_result_to_dataframe(self):
        point_csv_df = self.spark.\
            read.\
            format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
                area_lm_point_input_location
        )
        point_csv_df.createOrReplaceTempView("pointtable")
        point_df = self.spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")

        point_rdd = Adapter.toSpatialRdd(point_df, "arealandmark")
        point_rdd.analyze()

        polygon_wkt_df = self.spark.read.\
            format("csv").\
            option("delimiter", "\t").\
            option("header", "false").load(
                mixed_wkt_geometry_input_location
        )

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_df = self.spark.\
            sql("select ST_GeomFromWKT(polygontable._c0) as usacounty from polygontable")

        polygon_rdd = Adapter.toSpatialRdd(polygon_df, "usacounty")
        polygon_rdd.analyze()
        circle_rdd = CircleRDD(polygon_rdd, 0.2)

        point_rdd.spatialPartitioning(GridType.QUADTREE)
        circle_rdd.spatialPartitioning(point_rdd.getPartitioner())

        point_rdd.buildIndex(IndexType.QUADTREE, True)

        join_result_pair_rdd = JoinQuery.\
            DistanceJoinQueryFlat(point_rdd, circle_rdd, True, True)

        join_result_df = Adapter.toDf(join_result_pair_rdd, self.spark)
        join_result_df.printSchema()
        join_result_df.show()

    def test_load_id_column_data_check(self):
        spatial_rdd = PolygonRDD(self.spark.sparkContext, geojson_id_input_location, FileDataSplitter.GEOJSON, True)
        spatial_rdd.analyze()
        df = Adapter.toDf(spatial_rdd, self.spark)
        df.show()
        try:
            assert df.columns.__len__() == 3
        except AssertionError:
            assert df.columns.__len__() == 4
        assert df.count() == 1

    def _create_spatial_point_table(self) -> DataFrame:
        df = self.spark.read.\
            format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(area_lm_point_input_location)

        df.createOrReplaceTempView("inputtable")

        spatial_df = self.spark.sql("select ST_PointFromText(inputtable._c0,\",\") as geom from inputtable")

        return spatial_df

    def test_to_spatial_rdd_df_and_geom_field_name(self):
        spatial_df = self._create_spatial_point_table()

        spatial_rdd = Adapter.toSpatialRdd(spatial_df, "geom")
        spatial_rdd = Adapter.toSpatialRdd(spatial_df, "s")
        spatial_rdd.analyze()

        assert spatial_rdd.approximateTotalCount == 121960
        assert spatial_rdd.boundaryEnvelope == Envelope(-179.147236, 179.475569, -14.548699, 71.35513400000001)

    def test_to_spatial_rdd_df_with_non_geom_fields(self):
        spatial_df = self._create_spatial_point_table()
        spatial_df = spatial_df.withColumn("i", expr("10")).withColumn("s", expr("'20'"))
        spatial_rdd = Adapter.toSpatialRdd(spatial_df, "geom")
        assert spatial_rdd.fieldNames == ['i', 's']
        spatial_rdd.analyze()
        assert spatial_rdd.approximateTotalCount == 121960
        assert spatial_rdd.boundaryEnvelope == Envelope(-179.147236, 179.475569, -14.548699, 71.35513400000001)

    def test_to_spatial_rdd_df_with_custom_user_data_field_names(self):
        spatial_df = self._create_spatial_point_table()
        spatial_df = spatial_df.withColumn("i", expr("10")).withColumn("s", expr("'20'"))
        spatial_rdd = Adapter.toSpatialRdd(spatial_df, "geom", ["i2", "s2"])
        assert spatial_rdd.fieldNames == ['i2', 's2']
        spatial_rdd.analyze()
        assert spatial_rdd.approximateTotalCount == 121960
        assert spatial_rdd.boundaryEnvelope == Envelope(-179.147236, 179.475569, -14.548699, 71.35513400000001)

    def test_to_spatial_rdd_df(self):
        spatial_df = self._create_spatial_point_table()

        spatial_rdd = Adapter.toSpatialRdd(spatial_df, "geometry")

        spatial_rdd.analyze()

        assert spatial_rdd.approximateTotalCount == 121960
        assert spatial_rdd.boundaryEnvelope == Envelope(-179.147236, 179.475569, -14.548699, 71.35513400000001)

    @pytest.mark.skipif(is_greater_or_equal_version(version, "1.0.0"), reason="Deprecated in Sedona")
    def test_to_spatial_rdd_df_geom_column_id(self):
        df = self.spark.read.\
            format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(mixed_wkt_geometry_input_location)

        df_shorter = df.select(col("_c0").alias("geom"), col("_c6").alias("county_name"))
        df_shorter.createOrReplaceTempView("county_data")

        spatial_df = self.spark.sql("SELECT ST_GeomFromWKT(geom) as geom, county_name FROM county_data")
        spatial_df.show()

    def test_to_df_srdd_fn_spark(self):
        spatial_rdd = PolygonRDD(
            self.spark.sparkContext, geojson_input_location, FileDataSplitter.GEOJSON, True
        )
        spatial_rdd.analyze()
        assert spatial_rdd.approximateTotalCount == 1001

        spatial_columns = [
                "state_id", "county_id", "tract_id", "bg_id",
                "fips", "fips_short", "bg_nr", "type", "code1", "code2"
            ]
        spatial_df = Adapter.toDf(
            spatial_rdd,
            spatial_columns,
            self.spark
        )

        spatial_df.show()

        assert spatial_df.columns == ["geometry", *spatial_columns]
        assert spatial_df.count() == 1001
