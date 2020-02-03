import os

import pyspark

from geo_pyspark.core.jvm.config import compare_versions, GeoSparkMeta

from geo_pyspark.core.formatMapper.geo_json_reader import GeoJsonReader
from tests.test_base import TestBase
from tests.tools import tests_path

geo_json_contains_id = os.path.join(tests_path, "resources/testContainsId.json")
geo_json_geom_with_feature_property = os.path.join(tests_path, "resources/testPolygon.json")
geo_json_geom_without_feature_property = os.path.join(tests_path, "resources/testpolygon-no-property.json")
geo_json_with_invalid_geometries = os.path.join(tests_path, "resources/testInvalidPolygon.json")
geo_json_with_invalid_geom_with_feature_property = os.path.join(tests_path, "resources/invalidSyntaxGeometriesJson.json")


class TestGeoJsonReader(TestBase):

    def test_read_to_geometry_rdd(self):
        if compare_versions(GeoSparkMeta.version, "1.2.0"):
            geo_json_rdd = GeoJsonReader.readToGeometryRDD(
                self.sc,
                geo_json_geom_with_feature_property
            )

            assert geo_json_rdd.rawSpatialRDD.count() == 1001

            geo_json_rdd = GeoJsonReader.readToGeometryRDD(
                self.sc,
                geo_json_geom_without_feature_property
            )

            assert geo_json_rdd.rawSpatialRDD.count() == 10

    def test_read_to_valid_geometry_rdd(self):
        if compare_versions(GeoSparkMeta.version, "1.2.0"):
            geo_json_rdd = GeoJsonReader.readToGeometryRDD(
                self.sc,
                geo_json_geom_with_feature_property,
                True,
                False
            )

            assert geo_json_rdd.rawSpatialRDD.count() == 1001

            geo_json_rdd = GeoJsonReader.readToGeometryRDD(
                self.sc,
                geo_json_geom_without_feature_property,
                True,
                False
            )

            assert geo_json_rdd.rawSpatialRDD.count() == 10

            geo_json_rdd = GeoJsonReader.readToGeometryRDD(
                self.sc,
                geo_json_with_invalid_geometries,
                False,
                False
            )

            assert geo_json_rdd.rawSpatialRDD.count() == 2

            geo_json_rdd = GeoJsonReader.readToGeometryRDD(
                self.sc,
                geo_json_with_invalid_geometries
            )
            assert geo_json_rdd.rawSpatialRDD.count() == 3

    def test_read_to_include_id_rdd(self):
        if compare_versions(GeoSparkMeta.version, "1.2.0"):
            geo_json_rdd = GeoJsonReader.readToGeometryRDD(
                self.sc,
                geo_json_contains_id,
                True,
                False
            )

            geo_json_rdd = GeoJsonReader.readToGeometryRDD(
                sc=self.sc,
                inputPath=geo_json_contains_id,
                allowInvalidGeometries=True,
                skipSyntacticallyInvalidGeometries=False
            )
            assert geo_json_rdd.rawSpatialRDD.count() == 1
            try:
                assert geo_json_rdd.fieldNames.__len__() == 2
            except AssertionError:
                assert geo_json_rdd.fieldNames.__len__() == 3

    def test_read_to_geometry_rdd_invalid_syntax(self):
        if compare_versions(GeoSparkMeta.version, "1.2.0"):
            geojson_rdd = GeoJsonReader.readToGeometryRDD(
                self.sc,
                geo_json_with_invalid_geom_with_feature_property,
                False,
                True
            )

            assert geojson_rdd.rawSpatialRDD.count() == 1
