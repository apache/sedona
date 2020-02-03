import os

from geo_pyspark.core.formatMapper import WktReader
from tests.test_base import TestBase
from tests.tools import tests_path


class TestWktReader(TestBase):

    def test_read_to_geometry_rdd(self):
        wkt_geometries = os.path.join(tests_path, "resources/county_small.tsv")
        wkt_rdd = WktReader.readToGeometryRDD(self.sc, wkt_geometries, 0, True, False)
        assert wkt_rdd.rawSpatialRDD.count() == 103
        print(wkt_rdd.rawSpatialRDD.collect())
