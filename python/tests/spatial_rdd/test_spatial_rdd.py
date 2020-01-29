import os

import pyspark
import pytest
from pyspark import StorageLevel, RDD
from shapely.geometry import Point

from geo_pyspark.core.SpatialRDD import PointRDD
from geo_pyspark.core.enums import FileDataSplitter, GridType, IndexType
from geo_pyspark.core.formatMapper.geo_json_reader import GeoJsonReader
from geo_pyspark.core.geom.envelope import Envelope
from tests.test_base import TestBase
from tests.tools import tests_path

input_file_location = os.path.join(tests_path, "resources/arealm-small.csv")
crs_test_point = os.path.join(tests_path, "resources/crs-test-point.csv")
geo_json_contains_id = os.path.join(tests_path, "resources/testContainsId.json")

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
            newLevel=StorageLevel.MEMORY_ONLY
        )
        return spatial_rdd

    def test_analyze(self):
        spatial_rdd = self.create_spatial_rdd()
        assert spatial_rdd.analyze()

    def test_crs_transform(self):
        spatial_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=crs_test_point,
            Offset=0,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.CRSTransform("epsg:4326", "epsg:3857")

        assert spatial_rdd.rawSpatialRDD.collect()[0].geom.wkt == "POINT (-9833016.710450118 3805934.914254189)"

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

        assert envelope == Envelope(minx=-173.120769, maxx=-84.965961, miny=30.244859, maxy=71.355134)

    def test_boundary_envelope(self):
        spatial_rdd = self.create_spatial_rdd()
        spatial_rdd.analyze()
        assert Envelope(
            minx=-173.120769, maxx=-84.965961, miny=30.244859, maxy=71.355134) == spatial_rdd.boundaryEnvelope

    def test_build_index(self):
        for grid_type in GridType:
            spatial_rdd = self.create_spatial_rdd()
            spatial_rdd.spatialPartitioning(grid_type)
            spatial_rdd.buildIndex(IndexType.QUADTREE, True)
            spatial_rdd.buildIndex(IndexType.QUADTREE, False)
            spatial_rdd.buildIndex(IndexType.RTREE, True)
            spatial_rdd.buildIndex(IndexType.RTREE, False)

    def test_count_without_duplicates(self):
        spatial_rdd = self.create_spatial_rdd()
        assert spatial_rdd.countWithoutDuplicates() == 2996

    def test_field_names(self):
        spatial_rdd = self.create_spatial_rdd()
        assert spatial_rdd.fieldNames == []
        geo_json_rdd = GeoJsonReader.readToGeometryRDD(
            self.sc,
            geo_json_contains_id,
            True,
            False
        )
        try:
            assert geo_json_rdd.fieldNames == ['zipcode', 'name']
        except AssertionError:
            assert geo_json_rdd.fieldNames == ['id', 'zipcode', 'name']

    def test_get_crs_transformation(self):
        spatial_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=crs_test_point,
            Offset=0,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        assert not spatial_rdd.getCRStransformation()
        spatial_rdd.CRSTransform("epsg:4326", "epsg:3857")

        assert spatial_rdd.getCRStransformation()

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

    def test_get_source_epsg_code(self):
        spatial_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=crs_test_point,
            Offset=0,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        assert spatial_rdd.getSourceEpsgCode() == ""

        spatial_rdd.CRSTransform("epsg:4326", "epsg:3857")

        assert spatial_rdd.getSourceEpsgCode() == "epsg:4326"

    def test_get_target_epsg_code(self):
        spatial_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=crs_test_point,
            Offset=0,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        assert spatial_rdd.getTargetEpsgCode() == ""

        spatial_rdd.CRSTransform("epsg:4326", "epsg:3857")

        assert spatial_rdd.getTargetEpsgCode() == "epsg:3857"

    def test_grids(self):

        for grid_type in GridType:
            spatial_rdd = self.create_spatial_rdd()
            spatial_rdd.spatialPartitioning(grid_type)
            grids = spatial_rdd.grids
            if grid_type != GridType.QUADTREE and grid_type != GridType.KDBTREE:
                assert grids is not None
                assert isinstance(grids, list)
                assert isinstance(grids[0], Envelope)
            else:
                assert grids is None

    def test_indexed_rdd(self):
        pass

    def test_indexed_raw_rdd(self):
        pass

    def test_partition_tree(self):
        spatial_rdd = self.create_spatial_rdd()
        with pytest.raises(AttributeError):
            spatial_rdd.buildIndex(IndexType.QUADTREE, True)

        spatial_rdd.spatialPartitioning(GridType.QUADTREE)

        print(spatial_rdd.partitionTree)
