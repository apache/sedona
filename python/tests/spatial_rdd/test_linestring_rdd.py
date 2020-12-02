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

from pyspark import StorageLevel

from sedona.core.SpatialRDD import LineStringRDD
from sedona.core.enums import IndexType, GridType
from sedona.core.geom.envelope import Envelope
from tests.properties.linestring_properties import input_count, input_boundary, input_location, splitter, num_partitions, \
    grid_type, transformed_envelope, input_boundary_2, transformed_envelope_2
from tests.test_base import TestBase


class TestLineStringRDD(TestBase):

    def compare_count(self, spatial_rdd: LineStringRDD, envelope: Envelope, count: int):

        spatial_rdd.analyze()

        assert count == spatial_rdd.approximateTotalCount
        assert envelope == spatial_rdd.boundaryEnvelope

    def test_constructor(self):
        spatial_rdd_core = LineStringRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            splitter=splitter,
            carryInputData=True,
            partitions=num_partitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        self.compare_count(spatial_rdd_core, input_boundary, input_count)

        spatial_rdd = LineStringRDD()

        spatial_rdd_core = LineStringRDD(
            self.sc,
            input_location,
            splitter,
            True,
            num_partitions,
            StorageLevel.MEMORY_ONLY
        )
        self.compare_count(spatial_rdd_core, input_boundary, input_count)

        spatial_rdd = LineStringRDD(spatial_rdd_core.rawJvmSpatialRDD)

        self.compare_count(spatial_rdd, input_boundary, input_count)

        spatial_rdd = LineStringRDD(spatial_rdd_core.rawJvmSpatialRDD, "epsg:4326", "epsg:5070")

        self.compare_count(spatial_rdd, transformed_envelope, input_count)

        spatial_rdd = LineStringRDD(self.sc, input_location, 0, 3, splitter, True, num_partitions)

        self.compare_count(spatial_rdd, input_boundary_2, input_count)

        spatial_rdd = LineStringRDD(self.sc, input_location, 0, 3, splitter, True)

        self.compare_count(spatial_rdd, input_boundary_2, input_count)

        spatial_rdd = LineStringRDD(self.sc, input_location, splitter, True, num_partitions)

        self.compare_count(spatial_rdd, input_boundary, input_count)

        spatial_rdd = LineStringRDD(self.sc, input_location, splitter, True)

        self.compare_count(spatial_rdd, input_boundary, input_count)

        spatial_rdd = LineStringRDD(spatial_rdd_core.rawJvmSpatialRDD, StorageLevel.MEMORY_ONLY)

        self.compare_count(spatial_rdd, input_boundary, input_count)

        spatial_rdd = LineStringRDD(self.sc, input_location, 0, 3, splitter, True, num_partitions, StorageLevel.MEMORY_ONLY)

        self.compare_count(spatial_rdd, input_boundary_2, input_count)

        spatial_rdd = LineStringRDD(self.sc, input_location, 0, 3, splitter, True,
                                    StorageLevel.MEMORY_ONLY)

        self.compare_count(spatial_rdd, input_boundary_2, input_count)

        spatial_rdd = LineStringRDD(self.sc, input_location, splitter, True, num_partitions,
                                    StorageLevel.MEMORY_ONLY)

        self.compare_count(spatial_rdd, input_boundary, input_count)

        spatial_rdd = LineStringRDD(self.sc, input_location, splitter, True, StorageLevel.MEMORY_ONLY)

        self.compare_count(spatial_rdd, input_boundary, input_count)

        spatial_rdd = LineStringRDD(spatial_rdd_core.rawJvmSpatialRDD, StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:5070")

        self.compare_count(spatial_rdd, transformed_envelope, input_count)

        spatial_rdd = LineStringRDD(self.sc, input_location, 0, 3, splitter, True, num_partitions,
                                    StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:5070")

        self.compare_count(spatial_rdd, transformed_envelope_2, input_count)

        spatial_rdd = LineStringRDD(self.sc, input_location, 0, 3, splitter, True,
                                    StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:5070")

        self.compare_count(spatial_rdd, transformed_envelope_2, input_count)

        spatial_rdd = LineStringRDD(self.sc, input_location, splitter, True, num_partitions,
                                    StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:5070")

        self.compare_count(spatial_rdd, transformed_envelope, input_count)

        spatial_rdd = LineStringRDD(self.sc, input_location, splitter, True,
                                    StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:5070")

        self.compare_count(spatial_rdd, transformed_envelope, input_count)


    def test_empty_constructor(self):
        spatial_rdd = LineStringRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            splitter=splitter,
            carryInputData=True,
            partitions=num_partitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(grid_type)
        spatial_rdd.buildIndex(IndexType.RTREE, True)
        spatial_rdd_copy = LineStringRDD()
        spatial_rdd_copy.rawJvmSpatialRDD = spatial_rdd.rawJvmSpatialRDD
        spatial_rdd_copy.analyze()

    def test_build_index_without_set_grid(self):
        spatial_rdd = LineStringRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            splitter=splitter,
            carryInputData=True,
            partitions=num_partitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()
        spatial_rdd.buildIndex(IndexType.RTREE, False)

    def test_mbr(self):
        linestring_rdd = LineStringRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            splitter=splitter,
            carryInputData=True,
            partitions=num_partitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        rectangle_rdd = linestring_rdd.MinimumBoundingRectangle()
        result = rectangle_rdd.rawSpatialRDD.collect()

        for el in result:
            print(el)

        assert result.__len__() > -1
