from pyspark import StorageLevel

from geospark.core.SpatialRDD import PointRDD, CircleRDD
from tests.test_base import TestBase
from tests.properties.point_properties import input_location, offset, splitter, num_partitions


class TestCircleRDD(TestBase):

    def test_circle_rdd(self):
        spatial_rdd = PointRDD(
            self.sc,
            input_location,
            offset,
            splitter,
            True,
            num_partitions,
            StorageLevel.MEMORY_ONLY
        )

        circle_rdd = CircleRDD(spatial_rdd, 0.5)

        circle_rdd.analyze()

        assert circle_rdd.approximateTotalCount == 3000

        assert circle_rdd.rawSpatialRDD.take(1)[0].getUserData() == "testattribute0\ttestattribute1\ttestattribute2"
        assert circle_rdd.rawSpatialRDD.take(1)[0].geom.radius == 0.5
