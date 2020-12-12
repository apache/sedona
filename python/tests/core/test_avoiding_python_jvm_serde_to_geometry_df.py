from pyspark.sql.types import StructType, StructField, StringType

from sedona.core.SpatialRDD import CircleRDD
from sedona.core.enums import GridType
from sedona.core.enums import IndexType
from sedona.core.formatMapper import WktReader
from sedona.core.spatialOperator.join_params import JoinParams
from sedona.core.spatialOperator.join_query_raw import JoinQueryRaw
from sedona.core.spatialOperator.range_query_raw import RangeQueryRaw
from sedona.sql.types import GeometryType
from tests.test_base import TestBase

import os

from tests.tools import tests_path
from shapely.wkt import loads

bank_csv_path = os.path.join(tests_path, "resources/small/points.csv")
areas_csv_path = os.path.join(tests_path, "resources/small/areas.csv")


class TestOmitPythonJvmSerdeToGeometryDf(TestBase):
    expected_pois_within_areas_ids = [['4', '4'], ['1', '6'], ['2', '1'], ['3', '3'], ['3', '7']]

    def test_spatial_join_to_geometry_df(self):
        poi_point_rdd = WktReader.readToGeometryRDD(self.sc, bank_csv_path, 1, False, False)
        areas_polygon_rdd = WktReader.readToGeometryRDD(self.sc, areas_csv_path, 1, False, False)
        poi_point_rdd.analyze()
        areas_polygon_rdd.analyze()

        poi_point_rdd.spatialPartitioning(GridType.QUADTREE)
        areas_polygon_rdd.spatialPartitioning(poi_point_rdd.getPartitioner())

        jvm_sedona_rdd = JoinQueryRaw.spatialJoin(poi_point_rdd, areas_polygon_rdd, JoinParams())
        geometry_df_sedona = jvm_sedona_rdd.to_geometry_df(spark=self.spark,
                                                           left_field_names=["area_geom", "area_user_data"],
                                                           right_field_names=["poi_geom", "poi_user_data"])

        assert geometry_df_sedona.count() == 5
        assert geometry_df_sedona.columns == ["area_geom", "area_user_data", "poi_geom", "poi_user_data"]

        assert geometry_df_sedona.schema == StructType(
            [
                StructField("area_geom", GeometryType()),
                StructField("area_user_data", StringType()),
                StructField("poi_geom", GeometryType()),
                StructField("poi_user_data", StringType())
            ]
        )

    def test_distance_join_query_flat_to_geometry_df(self):
        poi_point_rdd = WktReader.readToGeometryRDD(self.sc, bank_csv_path, 1, False, False)
        circle_rdd = CircleRDD(poi_point_rdd, 2.0)

        circle_rdd.analyze()
        poi_point_rdd.analyze()

        poi_point_rdd.spatialPartitioning(GridType.QUADTREE)
        circle_rdd.spatialPartitioning(poi_point_rdd.getPartitioner())

        jvm_sedona_rdd = JoinQueryRaw.DistanceJoinQueryFlat(poi_point_rdd, circle_rdd, False, True)

        geometry_df_sedona_rdd = jvm_sedona_rdd.to_geometry_df(
            self.spark,
            left_field_names=["poi_from_geom", "poi_from_user_data"],
            right_field_names=["poi_to_geom", "poi_to_user_data"]
        )

        assert geometry_df_sedona_rdd.count() == 10
        assert geometry_df_sedona_rdd.columns == [
            "poi_from_geom", "poi_from_user_data",
            "poi_to_geom", "poi_to_user_data"
        ]

        assert geometry_df_sedona_rdd.schema == StructType(
            [
                StructField("poi_from_geom", GeometryType()),
                StructField("poi_from_user_data", StringType()),
                StructField("poi_to_geom", GeometryType()),
                StructField("poi_to_user_data", StringType())
            ]
        )

    def test_spatial_join_query_flat_to_geometry_df(self):
        poi_point_rdd = WktReader.readToGeometryRDD(self.sc, bank_csv_path, 1, False, False)
        areas_polygon_rdd = WktReader.readToGeometryRDD(self.sc, areas_csv_path, 1, False, False)
        poi_point_rdd.analyze()
        areas_polygon_rdd.analyze()

        poi_point_rdd.spatialPartitioning(GridType.QUADTREE)
        areas_polygon_rdd.spatialPartitioning(poi_point_rdd.getPartitioner())

        jvm_sedona_rdd = JoinQueryRaw.SpatialJoinQueryFlat(
            poi_point_rdd, areas_polygon_rdd, False, True)

        pois_within_areas_with_default_column_names = jvm_sedona_rdd.to_geometry_df(self.spark)
        assert pois_within_areas_with_default_column_names.columns == ["_c0", "_c1", "_c2", "_c3"]

        assert pois_within_areas_with_default_column_names.count() == 5

        assert pois_within_areas_with_default_column_names.schema == StructType(
            [
                StructField("_c0", GeometryType()),
                StructField("_c1", StringType()),
                StructField("_c2", GeometryType()),
                StructField("_c3", StringType())
            ]
        )

        pois_within_areas_with_passed_column_names = jvm_sedona_rdd.to_geometry_df(
            spark=self.spark,
            left_field_names=["area_geom", "left_user_data"],
            right_field_names=["poi_geom", "right_user_data"]
        )

        assert pois_within_areas_with_passed_column_names.count() == 5

        assert pois_within_areas_with_passed_column_names.columns == ["area_geom", "left_user_data", "poi_geom",
                                                                      "right_user_data"]
        assert pois_within_areas_with_passed_column_names.schema == StructType(
            [
                StructField("area_geom", GeometryType()),
                StructField("left_user_data", StringType()),
                StructField("poi_geom", GeometryType()),
                StructField("right_user_data", StringType())
            ]
        )

    def test_range_query_flat_to_geometry_df(self):
        poi_point_rdd = WktReader.readToGeometryRDD(self.sc, bank_csv_path, 1, False, False)

        poi_point_rdd.analyze()

        poi_point_rdd.spatialPartitioning(GridType.QUADTREE)
        poi_point_rdd.buildIndex(IndexType.QUADTREE, False)

        result = RangeQueryRaw.SpatialRangeQuery(
            poi_point_rdd, loads("POLYGON((0 0, 0 20, 20 20, 20 0, 0 0))"), True, True
        )

        rdd = result.to_rdd()

        assert rdd.collect().__len__() == 4

        geometry_df_without_column_names = result.to_geometry_df(self.spark)

        assert geometry_df_without_column_names.count() == 4
        assert geometry_df_without_column_names.columns == ["_c0", "_c1"]
        assert geometry_df_without_column_names.schema == StructType(
            [
                StructField("_c0", GeometryType()),
                StructField("_c1", StringType())
            ]
        )
        geometry_df = result.to_geometry_df(self.spark, field_names=["poi_geom", "user_data"])

        assert geometry_df.count() == 4
        assert geometry_df.columns == ["poi_geom", "user_data"]
        assert geometry_df.schema == StructType(
            [
                StructField("poi_geom", GeometryType()),
                StructField("user_data", StringType())
            ]
        )
