import os
import shutil

import pytest
from pyspark import StorageLevel

from geo_pyspark.core.SpatialRDD import PointRDD
from geo_pyspark.core.enums import FileDataSplitter
from geo_pyspark.core.geom_types import Envelope
from tests.test_base import TestBase
from tests.tools import tests_path

wkb_folder = "wkb"
wkt_folder = "wkt"

test_save_as_wkb_with_data = os.path.join(tests_path, wkb_folder, "testSaveAsWKBWithData")
test_save_as_wkb = os.path.join(tests_path, wkb_folder, "testSaveAsWKB")
test_save_as_empty_wkb = os.path.join(tests_path, wkb_folder, "testSaveAsEmptyWKB")
test_save_as_wkt = os.path.join(tests_path, wkt_folder, "testSaveAsWKT")
test_save_as_wkt_with_data = os.path.join(tests_path, wkt_folder, "testSaveAsWKTWithData")

inputLocation = os.path.join(tests_path, "resources/arealm-small.csv")
queryWindowSet = os.path.join(tests_path, "zcta510-small.csv")
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


def remove_directory(path: str) -> bool:
    try:
        shutil.rmtree(path)
    except Exception as e:
        return False
    return True


@pytest.fixture
def remove_wkb_directory():
    remove_directory(test_save_as_wkb_with_data)


class TestSpatialRDDWriter(TestBase):

    def test_save_as_geo_json_with_data(self, remove_wkb_directory):
        spatial_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=inputLocation,
            Offset=offset,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.saveAsGeoJSON(test_save_as_wkb_with_data)

        result_wkb = PointRDD(
            sparkContext=self.sc,
            InputLocation=test_save_as_wkb_with_data,
            splitter=FileDataSplitter.GEOJSON,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        assert result_wkb.rawSpatialRDD.count() == spatial_rdd.rawSpatialRDD.count()
