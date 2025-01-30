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

import pyspark
import pytest
from pyspark import RDD
from shapely.geometry import Point
from tests.test_base import TestBase
from tests.tools import tests_resource

from sedona.core.enums import FileDataSplitter, GridType, IndexType
from sedona.core.formatMapper.geo_json_reader import GeoJsonReader
from sedona.utils.adapter import Adapter
from sedona.core.geom.envelope import Envelope
from sedona.core.SpatialRDD import PointRDD

input_file_location = os.path.join(tests_resource, "arealm-small.csv")
crs_test_point = os.path.join(tests_resource, "crs-test-point.csv")
geo_json_contains_id = os.path.join(tests_resource, "testContainsId.json")

offset = 1
splitter = FileDataSplitter.CSV
gridType = "rtree"
indexType = "rtree"
numPartitions = 11


class TestSpatialRDD(TestBase):

    def create_spatial_rdd(self):
        spatial_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=input_file_location,
            Offset=offset,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
        )
        return spatial_rdd

    def test_analyze(self):
        spatial_rdd = self.create_spatial_rdd()
        assert spatial_rdd.analyze()

    def test_minimum_bounding_rectangle(self):
        spatial_rdd = self.create_spatial_rdd()

        with pytest.raises(NotImplementedError):
            spatial_rdd.MinimumBoundingRectangle()

    def test_approximate_total_count(self):
        spatial_rdd = self.create_spatial_rdd()
        assert spatial_rdd.approximateTotalCount == 3000

    def test_boundary(self):
        spatial_rdd = self.create_spatial_rdd()
        envelope = spatial_rdd.boundary()

        assert envelope == Envelope(
            minx=-173.120769, maxx=-84.965961, miny=30.244859, maxy=71.355134
        )

    def test_boundary_envelope(self):
        spatial_rdd = self.create_spatial_rdd()
        spatial_rdd.analyze()
        assert (
            Envelope(minx=-173.120769, maxx=-84.965961, miny=30.244859, maxy=71.355134)
            == spatial_rdd.boundaryEnvelope
        )

    def test_build_index(self):
        for grid_type in GridType:
            spatial_rdd = self.create_spatial_rdd()
            spatial_rdd.spatialPartitioning(grid_type)
            spatial_rdd.buildIndex(IndexType.QUADTREE, True)
            spatial_rdd.buildIndex(IndexType.QUADTREE, False)
            spatial_rdd.buildIndex(IndexType.RTREE, True)
            spatial_rdd.buildIndex(IndexType.RTREE, False)

    def test_spatial_partitioning_with_number_of_partitions(self):
        for grid_type in GridType:
            spatial_rdd = self.create_spatial_rdd()
            spatial_rdd.spatialPartitioning(grid_type, 5)

    def test_count_without_duplicates(self):
        spatial_rdd = self.create_spatial_rdd()
        assert spatial_rdd.countWithoutDuplicates() == 2996

    def test_field_names(self):
        spatial_rdd = self.create_spatial_rdd()
        assert spatial_rdd.fieldNames == []
        geo_json_rdd = GeoJsonReader.readToGeometryRDD(
            self.sc, geo_json_contains_id, True, False
        )
        try:
            assert geo_json_rdd.fieldNames == ["zipcode", "name"]
        except AssertionError:
            assert geo_json_rdd.fieldNames == ["id", "zipcode", "name"]

    def test_get_partitioner(self):
        spatial_rdd = self.create_spatial_rdd()

        assert spatial_rdd.getPartitioner().name is None

        for grid_type in GridType:
            spatial_rdd.spatialPartitioning(grid_type)
            if grid_type == GridType.QUADTREE:
                assert spatial_rdd.getPartitioner().name == "QuadTreePartitioner"
            elif grid_type == GridType.KDBTREE:
                assert spatial_rdd.getPartitioner().name == "KDBTreePartitioner"
            else:
                assert spatial_rdd.getPartitioner().name == "FlatGridPartitioner"

            grids = spatial_rdd.getPartitioner().getGrids()
            assert len(grids) > 0
            assert all(isinstance(grid, Envelope) for grid in grids)

    def test_get_raw_spatial_rdd(self):
        spatial_rdd = self.create_spatial_rdd()
        assert isinstance(spatial_rdd.getRawSpatialRDD(), RDD)
        collected_to_python = spatial_rdd.getRawSpatialRDD().collect()
        geo_data = collected_to_python[0]
        assert geo_data.userData == "testattribute0\ttestattribute1\ttestattribute2"
        assert geo_data.geom == Point(-88.331492, 32.324142)

    def test_get_sample_number(self):
        spatial_rdd = self.create_spatial_rdd()
        assert spatial_rdd.getSampleNumber() == -1
        spatial_rdd.setSampleNumber(10)
        assert spatial_rdd.getSampleNumber() == 10

    def test_grids(self):

        for grid_type in GridType:
            spatial_rdd = self.create_spatial_rdd()
            spatial_rdd.spatialPartitioning(grid_type)

    def test_partition_tree(self):
        spatial_rdd = self.create_spatial_rdd()
        with pytest.raises(AttributeError):
            spatial_rdd.buildIndex(IndexType.QUADTREE, True)

        spatial_rdd.spatialPartitioning(GridType.QUADTREE)

        print(spatial_rdd.getPartitioner())

    def test_partition_unique(self):
        grids = [
            Envelope(0.0, 10.0, 0.0, 10.0),
            Envelope(10.0, 20.0, 0.0, 10.0),
            Envelope(0.0, 10.0, 10.0, 20.0),
            Envelope(10.0, 20.0, 10.0, 20.0),
        ]

        df = self.spark.createDataFrame(
            [("POLYGON ((5 5, 15 5, 15 15, 5 15, 5 5))",)], ["wkt"]
        ).selectExpr("ST_GeomFromText(wkt) as geometry")
        spatial_rdd = Adapter.toSpatialRdd(df, "geometry")

        spatial_rdd.spatialPartitioning(grids)
        assert spatial_rdd.spatialPartitionedRDD.count() == 5
        assert spatial_rdd.getPartitioner().getGrids() == grids

        spatial_rdd.spatialPartitioning(grids, introduce_duplicates=False)
        assert spatial_rdd.spatialPartitionedRDD.count() == 1
        spatial_rdd.getPartitioner().getGrids() == grids
