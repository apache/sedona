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

from sedona.core.SpatialRDD import PointRDD
from sedona.core.SpatialRDD.spatial_rdd import SpatialRDD
from sedona.core.enums import IndexType, GridType
from sedona.core.geom.envelope import Envelope
from tests.properties.point_properties import input_location, offset, splitter, num_partitions, input_count, input_boundary, \
    transformed_envelope, crs_point_test, crs_envelope, crs_envelope_transformed
from tests.test_base import TestBase


class TestPointRDD(TestBase):

    def compare_count(self, spatial_rdd: SpatialRDD, cnt: int, envelope: Envelope):
        spatial_rdd.analyze()
        assert cnt == spatial_rdd.approximateTotalCount
        assert envelope == spatial_rdd.boundaryEnvelope

    def test_constructor(self):
        spatial_rdd = PointRDD(
            self.sc,
            input_location,
            offset,
            splitter,
            True,
            num_partitions
        )
        spatial_rdd.rawSpatialRDD.take(9)[0].getUserData()
        assert spatial_rdd.rawSpatialRDD.take(9)[0].getUserData() == "testattribute0\ttestattribute1\ttestattribute2"
        assert spatial_rdd.rawSpatialRDD.take(9)[2].getUserData() == "testattribute0\ttestattribute1\ttestattribute2"
        assert spatial_rdd.rawSpatialRDD.take(9)[4].getUserData() == "testattribute0\ttestattribute1\ttestattribute2"
        assert spatial_rdd.rawSpatialRDD.take(9)[8].getUserData() == "testattribute0\ttestattribute1\ttestattribute2"

        spatial_rdd_copy = PointRDD(spatial_rdd.rawJvmSpatialRDD)
        self.compare_count(spatial_rdd_copy, input_count, input_boundary)
        spatial_rdd_copy = PointRDD(spatial_rdd.rawJvmSpatialRDD)
        self.compare_count(spatial_rdd_copy, input_count, input_boundary)
        spatial_rdd_copy = PointRDD(self.sc, input_location, offset, splitter, True, num_partitions)
        self.compare_count(spatial_rdd_copy, input_count, input_boundary)
        spatial_rdd_copy = PointRDD(self.sc, crs_point_test, splitter, True)
        self.compare_count(spatial_rdd_copy, 20000, crs_envelope)

    def test_empty_constructor(self):
        spatial_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            Offset=offset,
            splitter=splitter,
            carryInputData=True,
            partitions=num_partitions
        )
        spatial_rdd.buildIndex(IndexType.RTREE, False)
        spatial_rdd_copy = PointRDD()
        spatial_rdd_copy.rawJvmSpatialRDD = spatial_rdd.rawJvmSpatialRDD
        spatial_rdd_copy.analyze()

    def test_equal_partitioning(self):
        spatial_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            Offset=offset,
            splitter=splitter,
            carryInputData=False,
            partitions=10
        )
        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(GridType.QUADTREE)

        assert spatial_rdd.countWithoutDuplicates() == spatial_rdd.countWithoutDuplicatesSPRDD()

    def test_build_index_without_set_grid(self):
        spatial_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            Offset=offset,
            splitter=splitter,
            carryInputData=True,
            partitions=num_partitions
        )
        spatial_rdd.buildIndex(IndexType.RTREE, False)
