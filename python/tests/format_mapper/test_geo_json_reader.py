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

from sedona.core.jvm.config import is_greater_or_equal_version, SedonaMeta

from sedona.core.formatMapper.geo_json_reader import GeoJsonReader
from tests.test_base import TestBase
from tests.tools import tests_resource

geo_json_contains_id = os.path.join(tests_resource, "testContainsId.json")
geo_json_geom_with_feature_property = os.path.join(tests_resource, "testPolygon.json")
geo_json_geom_without_feature_property = os.path.join(tests_resource, "testpolygon-no-property.json")
geo_json_with_invalid_geometries = os.path.join(tests_resource, "testInvalidPolygon.json")
geo_json_with_invalid_geom_with_feature_property = os.path.join(tests_resource, "invalidSyntaxGeometriesJson.json")


class TestGeoJsonReader(TestBase):

    def test_read_to_geometry_rdd(self):
        if is_greater_or_equal_version(SedonaMeta.version, "1.0.0"):
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
        if is_greater_or_equal_version(SedonaMeta.version, "1.0.0"):
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
        if is_greater_or_equal_version(SedonaMeta.version, "1.0.0"):
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
        if is_greater_or_equal_version(SedonaMeta.version, "1.0.0"):
            geojson_rdd = GeoJsonReader.readToGeometryRDD(
                self.sc,
                geo_json_with_invalid_geom_with_feature_property,
                False,
                True
            )

            assert geojson_rdd.rawSpatialRDD.count() == 1
