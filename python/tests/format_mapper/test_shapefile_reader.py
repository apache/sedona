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

import pytest

from sedona.core.geom.envelope import Envelope
from sedona.core.jvm.config import SedonaMeta, is_greater_or_equal_version
from sedona.core.spatialOperator import RangeQuery
from tests.tools import tests_resource
from sedona.core.formatMapper.shapefileParser import ShapefileReader
from tests.test_base import TestBase

undefined_type_shape_location = os.path.join(tests_resource, "shapefiles/undefined")
polygon_shape_location = os.path.join(tests_resource, "shapefiles/polygon")


class TestShapeFileReader(TestBase):

    @pytest.mark.skip(reason="test data is too large")
    def test_shape_file_end_with_undefined_type(self):
        shape_rdd = ShapefileReader.readToGeometryRDD(
            sc=self.sc, inputPath=undefined_type_shape_location
        )

        assert shape_rdd.fieldNames == ['LGA_CODE16', 'LGA_NAME16', 'STE_CODE16', 'STE_NAME16', 'AREASQKM16']
        assert shape_rdd.getRawSpatialRDD().count() == 545

    def test_read_geometry_rdd(self):
        shape_rdd = ShapefileReader.readToGeometryRDD(
            self.sc, polygon_shape_location
        )
        assert shape_rdd.fieldNames == []
        assert shape_rdd.rawSpatialRDD.collect().__len__() == 10000

    def test_read_to_polygon_rdd(self):
        input_location = os.path.join(tests_resource, "shapefiles/polygon")
        spatial_rdd = ShapefileReader.readToPolygonRDD(self.sc, input_location)
        geometry_rdd = ShapefileReader.readToGeometryRDD(self.sc, input_location)
        window = Envelope(-180.0, 180.0, -90.0, 90.0)
        count = RangeQuery.SpatialRangeQuery(spatial_rdd, window, False, False).count()

        assert spatial_rdd.rawSpatialRDD.count() == count
        assert 'org.apache.sedona.core.spatialRDD.SpatialRDD' in geometry_rdd._srdd.toString()
        assert 'org.apache.sedona.core.spatialRDD.PolygonRDD' in spatial_rdd._srdd.toString()

    def test_read_to_linestring_rdd(self):
        input_location = os.path.join(tests_resource, "shapefiles/polyline")
        spatial_rdd = ShapefileReader.readToLineStringRDD(self.sc, input_location)
        geometry_rdd = ShapefileReader.readToGeometryRDD(self.sc, input_location)
        window = Envelope(-180.0, 180.0, -90.0, 90.0)
        count = RangeQuery.SpatialRangeQuery(spatial_rdd, window, False, False).count()
        assert spatial_rdd.rawSpatialRDD.count() == count
        assert 'org.apache.sedona.core.spatialRDD.SpatialRDD' in geometry_rdd._srdd.toString()
        assert 'org.apache.sedona.core.spatialRDD.LineStringRDD' in spatial_rdd._srdd.toString()

    def test_read_to_point_rdd(self):
        input_location = os.path.join(tests_resource, "shapefiles/point")
        spatial_rdd = ShapefileReader.readToPointRDD(self.sc, input_location)
        geometry_rdd = ShapefileReader.readToGeometryRDD(self.sc, input_location)
        window = Envelope(-180.0, 180.0, -90.0, 90.0)
        count = RangeQuery.SpatialRangeQuery(spatial_rdd, window, False, False).count()
        assert spatial_rdd.rawSpatialRDD.count() == count
        assert 'org.apache.sedona.core.spatialRDD.SpatialRDD' in geometry_rdd._srdd.toString()
        assert 'org.apache.sedona.core.spatialRDD.PointRDD' in spatial_rdd._srdd.toString()

    def test_read_to_point_rdd_multipoint(self):
        input_location = os.path.join(tests_resource, "shapefiles/multipoint")
        spatial_rdd = ShapefileReader.readToPointRDD(self.sc, input_location)
        geometry_rdd = ShapefileReader.readToGeometryRDD(self.sc, input_location)
        window = Envelope(-180.0, 180.0, -90.0, 90.0)
        count = RangeQuery.SpatialRangeQuery(spatial_rdd, window, False, False).count()
        assert spatial_rdd.rawSpatialRDD.count() == count
        assert 'org.apache.sedona.core.spatialRDD.SpatialRDD' in geometry_rdd._srdd.toString()
        assert 'org.apache.sedona.core.spatialRDD.PointRDD' in spatial_rdd._srdd.toString()
