from pyspark import StorageLevel

from geo_pyspark.core.SpatialRDD import LineStringRDD
from geo_pyspark.core.enums import IndexType, GridType
from geo_pyspark.core.geom_types import Envelope
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

    def test_hilbert_curve_spatial_partitioning(self):
        spatial_rdd = LineStringRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            splitter=splitter,
            carryInputData=True,
            partitions=10,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(GridType.HILBERT)
        for envelope in spatial_rdd.grids:
            print(envelope)

    def test_rtree_spatial_partitioning(self):
        spatial_rdd = LineStringRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            splitter=splitter,
            carryInputData=True,
            partitions=10,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(GridType.RTREE)
        for envelope in spatial_rdd.grids:
            print(envelope)

    def test_voronoi_spatial_partitioning(self):
        spatial_rdd = LineStringRDD(
            sparkContext=self.sc,
            InputLocation=input_location,
            splitter=splitter,
            carryInputData=True,
            partitions=10,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(GridType.VORONOI)
        for envelope in spatial_rdd.grids:
            print(envelope)

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
