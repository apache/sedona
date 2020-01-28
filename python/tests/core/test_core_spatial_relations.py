import os

from geo_pyspark.core.SpatialRDD import PointRDD, PolygonRDD
from geo_pyspark.core.enums import FileDataSplitter, GridType
from geo_pyspark.core.spatialOperator import JoinQuery
from tests.test_base import TestBase
from tests.tools import tests_path

point_path = os.path.join(tests_path, "resources/points.csv")
counties_path = os.path.join(tests_path, "resources/counties_tsv.csv")


class TestJoinQuery(TestBase):

    def test_spatial_join_query(self):
        point_rdd = PointRDD(
            self.sc,
            point_path,
            4,
            FileDataSplitter.WKT,
            True
        )

        polygon_rdd = PolygonRDD(
            self.sc,
            counties_path,
            2,
            3,
            FileDataSplitter.WKT,
            True
        )

        point_rdd.analyze()
        point_rdd.spatialPartitioning(GridType.KDBTREE)
        polygon_rdd.spatialPartitioning(point_rdd.getPartitioner())
        result = JoinQuery.SpatialJoinQuery(
            point_rdd,
            polygon_rdd,
            True,
            False
        )

        print(result.count())