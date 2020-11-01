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

from shapely.geometry import Polygon
from shapely.wkt import loads

from sedona.utils.geometry_adapter import GeometryAdapter
from tests.test_base import TestBase


class TestDirectSerialization(TestBase):

    def test_polygon(self):
        polygon = Polygon([(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)])
        jvm_geom = GeometryAdapter.create_jvm_geometry_from_base_geometry(self.sc._jvm, polygon)

        assert jvm_geom.toString() == "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))"

        ext = [(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]
        int = [(1, 1), (1, 1.5), (1.5, 1.5), (1.5, 1), (1, 1)]

        polygon = Polygon(ext, [int])
        jvm_geom = GeometryAdapter.create_jvm_geometry_from_base_geometry(self.sc._jvm, polygon)

        assert jvm_geom.toString() == "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0), (1 1, 1 1.5, 1.5 1.5, 1.5 1, 1 1))"

        wkt = "POLYGON ((-71.1776585052917 42.3902909739571, -71.1776820268866 42.3903701743239, -71.1776063012595 42.3903825660754, -71.1775826583081 42.3903033653531, -71.1776585052917 42.3902909739571))"
        polygon = loads(wkt)
        jvm_geom = GeometryAdapter.create_jvm_geometry_from_base_geometry(self.sc._jvm, polygon)

        assert jvm_geom.toString() == wkt

    def test_point(self):
        wkt = "POINT (-71.064544 42.28787)"
        point = loads(wkt)
        jvm_geom = GeometryAdapter.create_jvm_geometry_from_base_geometry(self.sc._jvm, point)
        assert jvm_geom.toString() == wkt

    def test_linestring(self):
        wkt = 'LINESTRING (-71.160281 42.258729, -71.160837 42.259113, -71.161144 42.25932)'
        linestring = loads(wkt)
        jvm_geom = GeometryAdapter.create_jvm_geometry_from_base_geometry(self.sc._jvm, linestring)
        assert jvm_geom.toString() == wkt

