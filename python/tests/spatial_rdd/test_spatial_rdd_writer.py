#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import os
import shutil

import pytest
from pyspark import StorageLevel

from sedona.core.SpatialRDD import PointRDD
from sedona.core.enums import FileDataSplitter
from sedona.core.geom.envelope import Envelope
from tests.test_base import TestBase
from tests.tools import tests_resource

wkb_folder = "wkb"
wkt_folder = "wkt"

test_save_as_wkb_with_data = os.path.join(tests_resource, wkb_folder, "testSaveAsWKBWithData")
test_save_as_wkb = os.path.join(tests_resource, wkb_folder, "testSaveAsWKB")
test_save_as_empty_wkb = os.path.join(tests_resource, wkb_folder, "testSaveAsEmptyWKB")
test_save_as_wkt = os.path.join(tests_resource, wkt_folder, "testSaveAsWKT")
test_save_as_wkt_with_data = os.path.join(tests_resource, wkt_folder, "testSaveAsWKTWithData")

inputLocation = os.path.join(tests_resource, "arealm-small.csv")
queryWindowSet = os.path.join(tests_resource, "zcta510-small.csv")
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

## todo add missing tests
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
