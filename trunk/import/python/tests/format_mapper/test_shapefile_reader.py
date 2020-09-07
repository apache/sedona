import os

from geospark.core.geom.envelope import Envelope
from geospark.core.jvm.config import GeoSparkMeta, is_greater_or_equal_version
from geospark.core.spatialOperator import RangeQuery
from tests.tools import tests_path
from geospark.core.formatMapper.shapefileParser import ShapefileReader
from tests.test_base import TestBase

undefined_type_shape_location = os.path.join(tests_path, "resources/shapefiles/undefined")
polygon_shape_location = os.path.join(tests_path, "resources/shapefiles/polygon")


class TestShapeFileReader(TestBase):

    def test_shape_file_end_with_undefined_type(self):
        shape_rdd = ShapefileReader.readToGeometryRDD(
            sc=self.sc, inputPath=undefined_type_shape_location
        )

        if is_greater_or_equal_version(GeoSparkMeta.version, "1.2.0"):
            assert shape_rdd.fieldNames == ['LGA_CODE16', 'LGA_NAME16', 'STE_CODE16', 'STE_NAME16', 'AREASQKM16']
        assert shape_rdd.getRawSpatialRDD().count() == 545

    def test_read_geometry_rdd(self):
        shape_rdd = ShapefileReader.readToGeometryRDD(
            self.sc, polygon_shape_location
        )
        assert shape_rdd.fieldNames == []
        assert shape_rdd.rawSpatialRDD.collect().__len__() == 10000

    def test_read_to_polygon_rdd(self):
        input_location = os.path.join(tests_path, "resources/shapefiles/polygon")
        spatial_rdd = ShapefileReader.readToPolygonRDD(self.sc, input_location)
        geometry_rdd = ShapefileReader.readToGeometryRDD(self.sc, input_location)
        window = Envelope(-180.0, 180.0, -90.0, 90.0)
        count = RangeQuery.SpatialRangeQuery(spatial_rdd, window, False, False).count()

        assert spatial_rdd.rawSpatialRDD.count() == count
        assert 'org.datasyslab.geospark.spatialRDD.SpatialRDD' in geometry_rdd._srdd.toString()
        assert 'org.datasyslab.geospark.spatialRDD.PolygonRDD' in spatial_rdd._srdd.toString()

    def test_read_to_linestring_rdd(self):
        input_location = os.path.join(tests_path, "resources/shapefiles/polyline")
        spatial_rdd = ShapefileReader.readToLineStringRDD(self.sc, input_location)
        geometry_rdd = ShapefileReader.readToGeometryRDD(self.sc, input_location)
        window = Envelope(-180.0, 180.0, -90.0, 90.0)
        count = RangeQuery.SpatialRangeQuery(spatial_rdd, window, False, False).count()
        assert spatial_rdd.rawSpatialRDD.count() == count
        assert 'org.datasyslab.geospark.spatialRDD.SpatialRDD' in geometry_rdd._srdd.toString()
        assert 'org.datasyslab.geospark.spatialRDD.LineStringRDD' in spatial_rdd._srdd.toString()

    def test_read_to_point_rdd(self):
        input_location = os.path.join(tests_path, "resources/shapefiles/point")
        spatial_rdd = ShapefileReader.readToPointRDD(self.sc, input_location)
        geometry_rdd = ShapefileReader.readToGeometryRDD(self.sc, input_location)
        window = Envelope(-180.0, 180.0, -90.0, 90.0)
        count = RangeQuery.SpatialRangeQuery(spatial_rdd, window, False, False).count()
        assert spatial_rdd.rawSpatialRDD.count() == count
        assert 'org.datasyslab.geospark.spatialRDD.SpatialRDD' in geometry_rdd._srdd.toString()
        assert 'org.datasyslab.geospark.spatialRDD.PointRDD' in spatial_rdd._srdd.toString()

    def test_read_to_point_rdd_multipoint(self):
        input_location = os.path.join(tests_path, "resources/shapefiles/multipoint")
        spatial_rdd = ShapefileReader.readToPointRDD(self.sc, input_location)
        geometry_rdd = ShapefileReader.readToGeometryRDD(self.sc, input_location)
        window = Envelope(-180.0, 180.0, -90.0, 90.0)
        count = RangeQuery.SpatialRangeQuery(spatial_rdd, window, False, False).count()
        assert spatial_rdd.rawSpatialRDD.count() == count
        assert 'org.datasyslab.geospark.spatialRDD.SpatialRDD' in geometry_rdd._srdd.toString()
        assert 'org.datasyslab.geospark.spatialRDD.PointRDD' in spatial_rdd._srdd.toString()
