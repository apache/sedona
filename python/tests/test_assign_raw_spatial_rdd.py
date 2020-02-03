from geo_pyspark.core.SpatialRDD import PointRDD
from tests.properties.point_properties import input_location, offset, splitter, num_partitions
from tests.test_base import TestBase
from pyspark import StorageLevel


class TestSpatialRddAssignment(TestBase):

    def test_raw_spatial_rdd_assignment(self):
        spatial_rdd = PointRDD(
            self.sc,
            input_location,
            offset,
            splitter,
            True,
            num_partitions,
            StorageLevel.MEMORY_ONLY
        )
        spatial_rdd.analyze()

        empty_point_rdd = PointRDD()
        empty_point_rdd.rawSpatialRDD = spatial_rdd.rawSpatialRDD
        empty_point_rdd.analyze()
        assert empty_point_rdd.countWithoutDuplicates() == spatial_rdd.countWithoutDuplicates()
        assert empty_point_rdd.boundaryEnvelope == spatial_rdd.boundaryEnvelope

        assert empty_point_rdd.rawSpatialRDD.map(lambda x: x.geom.area).collect()[0] == 0.0
        assert empty_point_rdd.rawSpatialRDD.take(9)[4].getUserData() == "testattribute0\ttestattribute1\ttestattribute2"
