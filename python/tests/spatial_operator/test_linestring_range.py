import os

from pyspark import StorageLevel

from geo_pyspark.core.SpatialRDD import LineStringRDD
from geo_pyspark.core.enums import IndexType, FileDataSplitter
from geo_pyspark.core.geom.envelope import Envelope
from geo_pyspark.core.spatialOperator import RangeQuery
from tests.test_base import TestBase
from tests.tools import tests_path

input_location = os.path.join(tests_path, "resources/primaryroads-linestring.csv")
offset = 0
splitter = FileDataSplitter.CSV
gridType = "rtree"
indexType = "rtree"


class TestLineStringRange(TestBase):
    loop_times = 5
    query_envelope = Envelope(-85.01, -60.01, 34.01, 50.01)

    def test_spatial_range_query(self):
        spatial_rdd = LineStringRDD(
            self.sc, input_location, splitter, True, StorageLevel.MEMORY_ONLY
        )
        for i in range(self.loop_times):
            result_size = RangeQuery.SpatialRangeQuery(spatial_rdd, self.query_envelope, False, False).count()
            assert result_size == 999

        assert RangeQuery.SpatialRangeQuery(
            spatial_rdd, self.query_envelope, False, False).take(10)[1].getUserData() is not None

    def test_spatial_range_query_using_index(self):
        spatial_rdd = LineStringRDD(
            self.sc, input_location, splitter, True, StorageLevel.MEMORY_ONLY
        )
        spatial_rdd.buildIndex(IndexType.RTREE, False)

        for i in range(self.loop_times):
            result_size = RangeQuery.SpatialRangeQuery(spatial_rdd, self.query_envelope, False, False).count()
            assert result_size == 999

        assert RangeQuery.SpatialRangeQuery(
            spatial_rdd, self.query_envelope, False, False).take(10)[1].getUserData() is not None
