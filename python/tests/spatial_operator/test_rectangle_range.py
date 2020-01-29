import os

from pyspark import StorageLevel

from geo_pyspark.core.SpatialRDD import RectangleRDD
from geo_pyspark.core.enums import IndexType, FileDataSplitter
from geo_pyspark.core.geom.envelope import Envelope
from geo_pyspark.core.spatialOperator import RangeQuery
from tests.test_base import TestBase
from tests.tools import tests_path

inputLocation = os.path.join(tests_path, "resources/zcta510-small.csv")
queryWindowSet = os.path.join(tests_path, "resources/zcta510-small.csv")
offset = 0
splitter = FileDataSplitter.CSV
gridType = "rtree"
indexType = "rtree"
numPartitions = 11
distance = 0.001
queryPolygonSet = os.path.join(tests_path, "resources/primaryroads-polygon.csv")
inputCount = 3000
inputBoundary = Envelope(-171.090042, 145.830505, -14.373765, 49.00127)
matchCount = 17599
matchWithOriginalDuplicatesCount = 17738


class TestRectangleRange(TestBase):
    query_envelope = Envelope(-90.01, -80.01, 30.01, 40.01)
    loop_times = 5

    def test_spatial_range_query(self):
        spatial_rdd = RectangleRDD(self.sc, inputLocation, offset, splitter, True, StorageLevel.MEMORY_ONLY)

        for i in range(self.loop_times):
            result_size = RangeQuery.SpatialRangeQuery(
                spatial_rdd, self.query_envelope, False, False).count()
            assert result_size == 193

        assert RangeQuery.SpatialRangeQuery(
            spatial_rdd, self.query_envelope, False, False).take(10)[1].getUserData() is not None

    def test_spatial_range_query_using_index(self):
        spatial_rdd = RectangleRDD(
            self.sc, inputLocation, offset, splitter, True, StorageLevel.MEMORY_ONLY)

        spatial_rdd.buildIndex(IndexType.RTREE, False)
        for i in range(self.loop_times):
            result_size = RangeQuery.SpatialRangeQuery(
                spatial_rdd, self.query_envelope, False, True).count()
            assert result_size == 193

        assert RangeQuery.SpatialRangeQuery(spatial_rdd, self.query_envelope, False, True).take(10)[1].getUserData()\
               is not None
