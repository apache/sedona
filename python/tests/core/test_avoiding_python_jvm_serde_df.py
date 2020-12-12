from sedona.core.SpatialRDD import CircleRDD
from sedona.core.enums import GridType, IndexType
from sedona.core.formatMapper import WktReader
from sedona.core.spatialOperator.join_params import JoinParams
from sedona.core.spatialOperator.join_query_raw import JoinQueryRaw
from sedona.core.spatialOperator.range_query_raw import RangeQueryRaw
from tests.test_base import TestBase

import os

from tests.tools import tests_path
from shapely.wkt import loads

bank_csv_path = os.path.join(tests_path, "resources/small/points.csv")
areas_csv_path = os.path.join(tests_path, "resources/small/areas.csv")


class TestOmitPythonJvmSerdeToDf(TestBase):
    expected_pois_within_areas_ids = [['4', '4'], ['1', '6'], ['2', '1'], ['3', '3'], ['3', '7']]

    def test_spatial_join_to_df(self):
        poi_point_rdd = WktReader.readToGeometryRDD(self.sc, bank_csv_path, 1, False, False)
        areas_polygon_rdd = WktReader.readToGeometryRDD(self.sc, areas_csv_path, 1, False, False)
        poi_point_rdd.analyze()
        areas_polygon_rdd.analyze()

        poi_point_rdd.spatialPartitioning(GridType.QUADTREE)
        areas_polygon_rdd.spatialPartitioning(poi_point_rdd.getPartitioner())

        jvm_sedona_rdd = JoinQueryRaw.spatialJoin(poi_point_rdd, areas_polygon_rdd, JoinParams())
        sedona_df = jvm_sedona_rdd.to_df(spark=self.spark,
                                         left_field_names=["area_geom", "area_id", "area_name"],
                                         right_field_names=["poi_geom", "poi_id", "poi_name"])

        assert sedona_df.count() == 5
        assert sedona_df.columns == ["area_geom", "area_id", "area_name", "poi_geom",
                                     "poi_id", "poi_name"]

    def test_distance_join_query_flat_to_df(self):
        poi_point_rdd = WktReader.readToGeometryRDD(self.sc, bank_csv_path, 1, False, False)
        circle_rdd = CircleRDD(poi_point_rdd, 2.0)

        circle_rdd.analyze()
        poi_point_rdd.analyze()

        poi_point_rdd.spatialPartitioning(GridType.QUADTREE)
        circle_rdd.spatialPartitioning(poi_point_rdd.getPartitioner())

        jvm_sedona_rdd = JoinQueryRaw.DistanceJoinQueryFlat(poi_point_rdd, circle_rdd, False, True)

        df_sedona_rdd = jvm_sedona_rdd.to_df(
            self.spark,
            left_field_names=["poi_from_geom", "poi_from_id", "poi_from_name"],
            right_field_names=["poi_to_geom", "poi_to_id", "poi_to_name"]
        )

        assert df_sedona_rdd.count() == 10
        assert df_sedona_rdd.columns == [
            "poi_from_geom",
            "poi_from_id",
            "poi_from_name",
            "poi_to_geom",
            "poi_to_id",
            "poi_to_name"
        ]

    def test_spatial_join_query_flat_to_df(self):
        poi_point_rdd = WktReader.readToGeometryRDD(self.sc, bank_csv_path, 1, False, False)
        areas_polygon_rdd = WktReader.readToGeometryRDD(self.sc, areas_csv_path, 1, False, False)
        poi_point_rdd.analyze()
        areas_polygon_rdd.analyze()

        poi_point_rdd.spatialPartitioning(GridType.QUADTREE)
        areas_polygon_rdd.spatialPartitioning(poi_point_rdd.getPartitioner())

        jvm_sedona_rdd = JoinQueryRaw.SpatialJoinQueryFlat(
            poi_point_rdd, areas_polygon_rdd, False, True)

        pois_within_areas_with_default_column_names = jvm_sedona_rdd.to_df(self.spark)

        assert pois_within_areas_with_default_column_names.count() == 5

        pois_within_areas_with_passed_column_names = jvm_sedona_rdd.to_df(
            spark=self.spark,
            left_field_names=["area_geom", "area_id", "area_name"],
            right_field_names=["poi_geom", "poi_id", "poi_name"]
        )

        assert pois_within_areas_with_passed_column_names.count() == 5

        assert pois_within_areas_with_passed_column_names.columns == ["area_geom", "area_id", "area_name", "poi_geom",
                                                                      "poi_id", "poi_name"]

        pois_within_areas_with_default_column_names.show()

        pois_within_area_default_column_names_ids = self.__row_to_list(
            pois_within_areas_with_default_column_names. \
                select("_c1", "_c4"). \
                collect()
        )
        pois_within_area_passed_column_names_ids = self.__row_to_list(
            pois_within_areas_with_passed_column_names. \
                select("area_id", "poi_id"). \
                collect()
        )

        assert pois_within_area_default_column_names_ids == self.expected_pois_within_areas_ids
        assert pois_within_area_passed_column_names_ids == self.expected_pois_within_areas_ids

    def test_range_query_flat_to_df(self):
        poi_point_rdd = WktReader.readToGeometryRDD(self.sc, bank_csv_path, 1, False, False)

        poi_point_rdd.analyze()

        poi_point_rdd.spatialPartitioning(GridType.QUADTREE)
        poi_point_rdd.buildIndex(IndexType.QUADTREE, False)

        result = RangeQueryRaw.SpatialRangeQuery(
            poi_point_rdd, loads("POLYGON((0 0, 0 20, 20 20, 20 0, 0 0))"), True, True
        )

        rdd = result.to_rdd()

        assert rdd.collect().__len__() == 4

        df_without_column_names = result.to_df(self.spark)

        assert df_without_column_names.count() == 4
        assert df_without_column_names.columns == ["geometry", "_c1", "_c2"]

        df = result.to_df(self.spark, field_names=["poi_geom", "poi_id", "poi_name"])

        assert df.count() == 4
        assert df.columns == ["poi_geom", "poi_id", "poi_name"]

    def __row_to_list(self, row_list):
        return [[*element] for element in row_list]
