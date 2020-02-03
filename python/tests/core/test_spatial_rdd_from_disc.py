import os
import shutil

import pytest
from pyspark import StorageLevel

from geo_pyspark.core.SpatialRDD import PointRDD, PolygonRDD, LineStringRDD
from geo_pyspark.core.enums import IndexType, GridType
from geo_pyspark.core.formatMapper.disc_utils import load_spatial_rdd_from_disc, \
    load_spatial_index_rdd_from_disc, GeoType
from geo_pyspark.core.spatialOperator import JoinQuery
from tests.test_base import TestBase
from tests.tools import tests_path


def remove_directory(path: str) -> bool:
    try:
        shutil.rmtree(path)
    except Exception as e:
        return False
    return True


disc_object_location = os.path.join(tests_path, "resources/spatial_objects/temp")
disc_location = os.path.join(tests_path, "resources/spatial_objects")


@pytest.fixture
def remove_spatial_rdd_disc_dir():
    remove_directory(disc_object_location)


class TestDiscUtils(TestBase):

    def test_saving_to_disc_spatial_rdd_point(self, remove_spatial_rdd_disc_dir):
        from tests.properties.point_properties import input_location, offset, splitter, num_partitions

        point_rdd = PointRDD(
            self.sc, input_location, offset, splitter, True, num_partitions, StorageLevel.MEMORY_ONLY
        )

        point_rdd.rawJvmSpatialRDD.saveAsObjectFile(os.path.join(disc_object_location, "point"))

    def test_saving_to_disc_spatial_rdd_polygon(self, remove_spatial_rdd_disc_dir):
        from tests.properties.polygon_properties import input_location, splitter, num_partitions
        polygon_rdd = PolygonRDD(
            self.sc,
            input_location,
            splitter,
            True,
            num_partitions,
            StorageLevel.MEMORY_ONLY
        )
        polygon_rdd.rawJvmSpatialRDD.saveAsObjectFile(os.path.join(disc_object_location, "polygon"))

    def test_saving_to_disc_spatial_rdd_linestring(self, remove_spatial_rdd_disc_dir):
        from tests.properties.linestring_properties import input_location, splitter, num_partitions
        linestring_rdd = LineStringRDD(
            self.sc,
            input_location,
            splitter,
            True,
            num_partitions,
            StorageLevel.MEMORY_ONLY
        )
        linestring_rdd.rawJvmSpatialRDD.saveAsObjectFile(os.path.join(disc_object_location, "line_string"))

    def test_saving_to_disc_index_point(self, remove_spatial_rdd_disc_dir):
        from tests.properties.linestring_properties import input_location, splitter, num_partitions
        linestring_rdd = LineStringRDD(
            self.sc,
            input_location,
            splitter,
            True,
            num_partitions,
            StorageLevel.MEMORY_ONLY
        )
        linestring_rdd.buildIndex(IndexType.RTREE, False)
        linestring_rdd.indexedRawRDD.saveAsObjectFile(os.path.join(disc_object_location, "line_string_index"))

    def test_saving_to_disc_index_polygon(self, remove_spatial_rdd_disc_dir):
        from tests.properties.polygon_properties import input_location, splitter, num_partitions
        polygon_rdd = PolygonRDD(
            self.sc,
            input_location,
            splitter,
            True,
            num_partitions,
            StorageLevel.MEMORY_ONLY
        )
        polygon_rdd.buildIndex(IndexType.RTREE, False)
        polygon_rdd.indexedRawRDD.saveAsObjectFile(os.path.join(disc_object_location, "polygon_index"))

    def test_saving_to_disc_index_linestring(self, remove_spatial_rdd_disc_dir):
        from tests.properties.point_properties import input_location, offset, splitter, num_partitions
        point_rdd = PointRDD(
            self.sc, input_location, offset, splitter, True, num_partitions, StorageLevel.MEMORY_ONLY
        )
        point_rdd.buildIndex(IndexType.RTREE, False)
        point_rdd.indexedRawRDD.saveAsObjectFile(os.path.join(disc_object_location, "point_index"))

    def test_loading_spatial_rdd_from_disc(self):
        point_rdd = load_spatial_rdd_from_disc(
            self.sc, os.path.join(disc_location, "point"), GeoType.POINT
        )
        point_index_rdd = load_spatial_index_rdd_from_disc(self.sc, os.path.join(disc_location, "point_index"))
        point_rdd.indexedRawRDD = point_index_rdd

        assert point_rdd.indexedRawRDD is not None
        assert isinstance(point_rdd, PointRDD)
        point_rdd.analyze()
        print(point_rdd.boundaryEnvelope)

        polygon_rdd = load_spatial_rdd_from_disc(
            self.sc, os.path.join(disc_location, "polygon"), GeoType.POLYGON
        )
        polygon_index_rdd = load_spatial_index_rdd_from_disc(self.sc, os.path.join(disc_location, "polygon_index"))
        polygon_rdd.indexedRawRDD = polygon_index_rdd
        polygon_rdd.analyze()

        print(polygon_rdd.boundaryEnvelope)

        assert polygon_rdd.indexedRawRDD is not None
        assert isinstance(polygon_rdd, PolygonRDD)

        linestring_rdd = load_spatial_rdd_from_disc(
            self.sc, os.path.join(disc_location, "line_string"), GeoType.LINESTRING
        )
        linestring_index_rdd = load_spatial_index_rdd_from_disc(self.sc, os.path.join(disc_location, "line_string_index"))
        linestring_rdd.indexedRawRDD = linestring_index_rdd

        assert linestring_rdd.indexedRawRDD is not None
        assert isinstance(linestring_rdd, LineStringRDD)

        linestring_rdd.analyze()
        print(linestring_rdd.boundaryEnvelope)

        linestring_rdd.spatialPartitioning(GridType.RTREE)
        polygon_rdd.spatialPartitioning(linestring_rdd.grids)
        polygon_rdd.buildIndex(IndexType.RTREE, True)
        linestring_rdd.buildIndex(IndexType.RTREE, True)

        result = JoinQuery.SpatialJoinQuery(
            linestring_rdd, polygon_rdd, True, True).collect()

        print(result)
