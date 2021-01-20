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

import pytest
from pyspark import StorageLevel

from sedona.core.SpatialRDD import RectangleRDD
from sedona.core.enums import IndexType, GridType, FileDataSplitter
from sedona.core.geom.envelope import Envelope
from tests.test_base import TestBase
from tests.tools import tests_resource

inputLocation = os.path.join(tests_resource, "zcta510-small.csv")
queryWindowSet = os.path.join(tests_resource, "zcta510-small.csv")
offset = 0
splitter = FileDataSplitter.CSV
gridType = "rtree"
indexType = "rtree"
numPartitions = 11
distance = 0.001
queryPolygonSet = os.path.join(tests_resource, "primaryroads-polygon.csv")
inputCount = 3000
inputBoundary = Envelope(minx=-171.090042, maxx=145.830505, miny=-14.373765, maxy=49.00127)
matchCount = 17599
matchWithOriginalDuplicatesCount = 17738


class TestRectangleRDD(TestBase):

    def test_constructor(self):
        spatial_rdd = RectangleRDD(
            sparkContext=self.sc,
            InputLocation=inputLocation,
            Offset=offset,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()

        assert inputCount == spatial_rdd.approximateTotalCount
        assert inputBoundary == spatial_rdd.boundaryEnvelope

        spatial_rdd = RectangleRDD(
            self.sc,
            inputLocation,
            offset,
            splitter,
            True,
            numPartitions,
            StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()

        assert inputCount == spatial_rdd.approximateTotalCount
        assert inputBoundary == spatial_rdd.boundaryEnvelope

    def test_empty_constructor(self):
        spatial_rdd = RectangleRDD(
            sparkContext=self.sc,
            InputLocation=inputLocation,
            Offset=offset,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()
        spatial_rdd.buildIndex(IndexType.RTREE, False)

        spatial_rdd_copy = RectangleRDD()
        spatial_rdd_copy.rawJvmSpatialRDD = spatial_rdd.rawJvmSpatialRDD
        spatial_rdd_copy.analyze()

    def test_build_index_without_set_grid(self):
        spatial_rdd = RectangleRDD(
            sparkContext=self.sc,
            InputLocation=inputLocation,
            Offset=offset,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions
        )

        spatial_rdd.analyze()

        spatial_rdd.buildIndex(IndexType.RTREE, False)

