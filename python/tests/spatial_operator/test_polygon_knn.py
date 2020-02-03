import os

from shapely.geometry import Point

from geo_pyspark.core.SpatialRDD import PolygonRDD
from geo_pyspark.core.enums import IndexType, FileDataSplitter
from geo_pyspark.core.spatialOperator import KNNQuery
from tests.test_base import TestBase
from tests.tools import tests_path, distance_sorting_functions

input_location = os.path.join(tests_path, "resources/primaryroads-polygon.csv")
splitter = FileDataSplitter.CSV
gridType = "rtree"
indexType = "rtree"


class TestPolygonKnn(TestBase):

    loop_times = 5
    top_k = 100
    query_point = Point(-84.01, 34.01)

    def test_spatial_knn_query(self):
        polygon_rdd = PolygonRDD(self.sc, input_location, splitter, True)

        for i in range(self.loop_times):
            result = KNNQuery.SpatialKnnQuery(polygon_rdd, self.query_point, self.top_k, False)
            assert result.__len__() > -1
            assert result[0].getUserData() is not None

    def test_spatial_knn_query_using_index(self):
        polygon_rdd = PolygonRDD(self.sc, input_location, splitter, True)
        polygon_rdd.buildIndex(IndexType.RTREE, False)
        for i in range(self.loop_times):
            result = KNNQuery.SpatialKnnQuery(polygon_rdd, self.query_point, self.top_k, True)
            assert result.__len__() > -1
            assert result[0].getUserData() is not None

    def test_spatial_knn_correctness(self):
        polygon_rdd = PolygonRDD(self.sc, input_location, splitter, True)
        result_no_index = KNNQuery.SpatialKnnQuery(polygon_rdd, self.query_point, self.top_k, False)
        polygon_rdd.buildIndex(IndexType.RTREE, False)
        result_with_index = KNNQuery.SpatialKnnQuery(polygon_rdd, self.query_point, self.top_k, True)

        sorted_result_no_index = sorted(result_no_index, key=lambda geo_data: distance_sorting_functions(
            geo_data, self.query_point))

        sorted_result_with_index = sorted(result_with_index, key=lambda geo_data: distance_sorting_functions(
            geo_data, self.query_point))

        difference = 0
        for x in range(self.top_k):
            difference += sorted_result_no_index[x].geom.distance(sorted_result_with_index[x].geom)

        assert difference == 0
