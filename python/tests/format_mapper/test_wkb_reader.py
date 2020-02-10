import os

from geo_pyspark.core.formatMapper.wkb_reader import WkbReader
from tests.test_base import TestBase
from tests.tools import tests_path


class TestWkbReader(TestBase):

    def test_read_to_geometry_rdd(self):
        wkb_geometries = os.path.join(tests_path, "resources/county_small_wkb.tsv")

        wkb_rdd = WkbReader.readToGeometryRDD(self.sc, wkb_geometries, 0, True, False)
        assert wkb_rdd.rawSpatialRDD.count() == 103
