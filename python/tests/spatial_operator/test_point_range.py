import os

from geo_pyspark.core.SpatialRDD import PointRDD
from geo_pyspark.core.enums import IndexType, FileDataSplitter
from geo_pyspark.core.geom_types import Envelope
from geo_pyspark.core.spatialOperator import RangeQuery
from tests.test_base import TestBase
from tests.tools import tests_path

input_location = os.path.join(tests_path, "resources/arealm-small.csv")
queryWindowSet = os.path.join("zcta510-small.csv")
offset = 1
splitter = FileDataSplitter.CSV
gridType = "rtree"
indexType = "rtree"
numPartitions = 11
distance = 0.01
queryPolygonSet = "primaryroads-polygon.csv"
inputCount = 3000
inputBoundary = Envelope(
    minx=-173.120769,
    maxx=-84.965961,
    miny=30.244859,
    maxy=71.355134
)
rectangleMatchCount = 103
rectangleMatchWithOriginalDuplicatesCount = 103
polygonMatchCount = 472
polygonMatchWithOriginalDuplicatesCount = 562


class TestPointRange(TestBase):
    loop_times = 5
    query_envelope = Envelope(-90.01, -80.01, 30.01, 40.01)

    def test_spatial_range_query(self):
        spatial_rdd = PointRDD(self.sc, input_location, offset, splitter, False)
        for i in range(self.loop_times):
            result_size = RangeQuery.\
                SpatialRangeQuery(spatial_rdd, self.query_envelope, False, False)\
                .count()
            assert result_size == 2830
        assert RangeQuery.SpatialRangeQuery(
            spatial_rdd, self.query_envelope, False, False).take(10)[1].\
                   getUserData() is not None

    def test_spatial_range_query_using_index(self):
        spatial_rdd = PointRDD(self.sc, input_location, offset, splitter, False)

        spatial_rdd.buildIndex(IndexType.RTREE, False)

        for i in range(self.loop_times):
            result_size = RangeQuery.\
                SpatialRangeQuery(spatial_rdd, self.query_envelope, False, False)\
                .count()
            assert result_size == 2830
        assert RangeQuery.SpatialRangeQuery(
            spatial_rdd, self.query_envelope, False, False).take(10)[1].\
                   getUserData() is not None
