import os

from shapely.geometry import Point

from geo_pyspark.core.SpatialRDD import PointRDD
from geo_pyspark.core.enums import IndexType, FileDataSplitter
from geo_pyspark.core.spatialOperator import KNNQuery
from tests.test_base import TestBase
from tests.tools import tests_path, distance_sorting_functions

input_location = os.path.join(tests_path, "resources/arealm-small.csv")
queryWindowSet = os.path.join("zcta510-small.csv")
offset = 1
splitter = FileDataSplitter.CSV
gridType = "rtree"
indexType = "rtree"


class TestPointKnn(TestBase):
    loop_times = 5
    query_point = Point(-84.01, 34.01)
    top_k = 100

    def test_spatial_knn_query(self):
        point_rdd = PointRDD(self.sc, input_location, offset, splitter, False)

        for i in range(self.loop_times):
            result = KNNQuery.SpatialKnnQuery(point_rdd, self.query_point, self.top_k, False)
            assert result.__len__() > -1
            assert result[0].getUserData() is not None

    def test_spatial_knn_query_using_index(self):
        point_rdd = PointRDD(self.sc, input_location, offset, splitter, False)
        point_rdd.buildIndex(IndexType.RTREE, False)

        for i in range(self.loop_times):
            result = KNNQuery.SpatialKnnQuery(point_rdd, self.query_point, self.top_k, False)
            assert result.__len__() > -1
            assert result[0].getUserData() is not None

    def test_spatial_knn_correctness(self):
        point_rdd = PointRDD(self.sc, input_location, offset, splitter, False)
        result_no_index = KNNQuery.SpatialKnnQuery(point_rdd, self.query_point, self.top_k, False)
        point_rdd.buildIndex(IndexType.RTREE, False)
        result_with_index = KNNQuery.SpatialKnnQuery(point_rdd, self.query_point, self.top_k, True)

        sorted_result_no_index = sorted(result_no_index, key=lambda geo_data: distance_sorting_functions(
            geo_data, self.query_point))

        sorted_result_with_index = sorted(result_with_index, key=lambda geo_data: distance_sorting_functions(
            geo_data, self.query_point))

        difference = 0
        for x in range(self.top_k):
            difference += sorted_result_no_index[x].geom.distance(sorted_result_with_index[x].geom)

        assert difference == 0
