from pyspark import StorageLevel

from geo_pyspark.core.SpatialRDD import PointRDD
from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.enums import IndexType, GridType
from geo_pyspark.core.geom.envelope import Envelope
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
        spatial_rdd_copy = PointRDD(spatial_rdd.rawJvmSpatialRDD, "epsg:4326", "epsg:5070")
        self.compare_count(spatial_rdd_copy, input_count, transformed_envelope)
        spatial_rdd_copy = PointRDD(self.sc, input_location, offset, splitter, True, num_partitions)
        self.compare_count(spatial_rdd_copy, input_count, input_boundary)
        spatial_rdd_copy = PointRDD(self.sc, crs_point_test, splitter, True)
        self.compare_count(spatial_rdd_copy, 20000, crs_envelope)
        spatial_rdd_copy = PointRDD(spatial_rdd.rawJvmSpatialRDD, StorageLevel.MEMORY_ONLY)
        self.compare_count(spatial_rdd_copy, input_count, input_boundary)
        spatial_rdd_copy = PointRDD(self.sc, input_location, offset, splitter, True, num_partitions, StorageLevel.MEMORY_ONLY)
        self.compare_count(spatial_rdd_copy, input_count, input_boundary)
        spatial_rdd_copy = PointRDD(self.sc, input_location, offset, splitter, True, StorageLevel.MEMORY_ONLY)
        self.compare_count(spatial_rdd_copy, input_count, input_boundary)
        spatial_rdd_copy = PointRDD(self.sc, crs_point_test, splitter, True, num_partitions, StorageLevel.MEMORY_ONLY)
        self.compare_count(spatial_rdd_copy, 20000, crs_envelope)
        spatial_rdd_copy = PointRDD(self.sc, crs_point_test, splitter, True, StorageLevel.MEMORY_ONLY)
        self.compare_count(spatial_rdd_copy, 20000, crs_envelope)
        spatial_rdd_copy = PointRDD(spatial_rdd.rawJvmSpatialRDD, StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:5070")
        self.compare_count(spatial_rdd_copy, input_count, transformed_envelope)
        spatial_rdd_copy = PointRDD(self.sc, input_location, offset, splitter, True, num_partitions,
                                    StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:5070")
        self.compare_count(spatial_rdd_copy, input_count, transformed_envelope)
        spatial_rdd_copy = PointRDD(self.sc, input_location, offset, splitter, True,
                                    StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:5070")
        self.compare_count(spatial_rdd_copy, input_count, transformed_envelope)
        spatial_rdd_copy = PointRDD(self.sc, crs_point_test, splitter, True,
                                    num_partitions, StorageLevel.MEMORY_ONLY, "epsg:4326", "epsg:5070")

        self.compare_count(spatial_rdd_copy, 20000, crs_envelope_transformed)
        spatial_rdd_copy = PointRDD(self.sc, crs_point_test, splitter, True, StorageLevel.MEMORY_ONLY,
                                    "epsg:4326", "epsg:5070")
        self.compare_count(spatial_rdd_copy, 20000, crs_envelope_transformed)

    def test_empty_constructor(self):
        spatial_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            Offset=offset,
            splitter=splitter,
            carryInputData=True,
            partitions=num_partitions,
            newLevel=StorageLevel.MEMORY_ONLY
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
            partitions=10,
            newLevel=StorageLevel.MEMORY_ONLY
        )
        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(GridType.EQUALGRID)

        for envelope in spatial_rdd.grids:
            print("PointRDD spatial partitioning grids: " + str(envelope))
        assert spatial_rdd.countWithoutDuplicates() == spatial_rdd.countWithoutDuplicatesSPRDD()

    def test_hilbert_curve_spatial_partitioning(self):
        spatial_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            Offset=offset,
            splitter=splitter,
            carryInputData=False,
            partitions=10,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(GridType.HILBERT)

        for envelope in spatial_rdd.grids:
            print(envelope)
        assert spatial_rdd.countWithoutDuplicates() == spatial_rdd.countWithoutDuplicatesSPRDD()

    def test_r_tree_spatial_partitioning(self):
        spatial_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            Offset=offset,
            splitter=splitter,
            carryInputData=True,
            partitions=10,
            newLevel=StorageLevel.MEMORY_ONLY
        )
        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(GridType.RTREE)

        for envelope in spatial_rdd.grids:
            print(envelope)

        assert spatial_rdd.countWithoutDuplicates() == spatial_rdd.countWithoutDuplicatesSPRDD()

    def test_voronoi_spatial_partitioning(self):
        spatial_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            Offset=offset,
            splitter=splitter,
            carryInputData=False,
            partitions=10,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(GridType.VORONOI)

        for envelope in spatial_rdd.grids:
            print(envelope)

        assert spatial_rdd.countWithoutDuplicates() == spatial_rdd.countWithoutDuplicatesSPRDD()

    def test_build_index_without_set_grid(self):
        spatial_rdd = PointRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            Offset=offset,
            splitter=splitter,
            carryInputData=True,
            partitions=num_partitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )
        spatial_rdd.buildIndex(IndexType.RTREE, False)
