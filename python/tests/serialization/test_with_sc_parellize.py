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

from shapely.geometry import Point, LineString, Polygon
from shapely.wkt import loads

from sedona.core.SpatialRDD import PointRDD, LineStringRDD, PolygonRDD
from sedona.utils.spatial_rdd_parser import GeoData
from tests.test_base import TestBase


class TestWithScParallelize(TestBase):

    def test_geo_data_convert_to_point_rdd(self):
        points = [
            GeoData(geom=Point(52.0, -21.0), userData="a"),
            GeoData(geom=Point(-152.4546, -23.1423), userData="b"),
            GeoData(geom=Point(62.253456, 221.2145), userData="c")
        ]

        rdd_data = self.sc.parallelize(points)
        point_rdd = PointRDD(rdd_data)

        collected_data = point_rdd.rawSpatialRDD.collect()
        sorted_collected_data = sorted(collected_data, key=lambda x: x.userData)
        assert all([geo1 == geo2 for geo1, geo2 in zip(points, sorted_collected_data)])

    def test_geo_data_convert_polygon_rdd(self):
        linestring = LineString([(0.0, 1.0), (1, 1), (12.0, 1.0)])
        wkt = 'LINESTRING (-71.160281 42.258729, -71.160837 42.259113, -71.161144 42.25932)'
        linestring2 = loads(wkt)

        linestrings = [
            GeoData(geom=linestring, userData="a"),
            GeoData(geom=linestring2, userData="b"),
        ]

        rdd_data = self.sc.parallelize(linestrings)

        linestring_rdd = LineStringRDD(rdd_data)
        collected_data = linestring_rdd.rawSpatialRDD.collect()
        sorted_collected_data = sorted(collected_data, key=lambda x: x.userData)
        assert all([geo1 == geo2 for geo1, geo2 in zip(linestrings, sorted_collected_data)])

    def test_geo_data_convert_linestring_rdd(self):
        polygon = Polygon([(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)])

        ext = [(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]
        int = [(1, 1), (1.5, 1), (1.5, 1.5), (1, 1.5), (1, 1)]

        polygon2 = Polygon(ext, [int])

        wkt = "POLYGON ((-71.1776585052917 42.3902909739571, -71.1776820268866 42.3903701743239, -71.1776063012595 42.3903825660754, -71.1775826583081 42.3903033653531, -71.1776585052917 42.3902909739571))"
        polygon3 = loads(wkt)

        polygons = [
                GeoData(geom=polygon, userData="a"),
                GeoData(geom=polygon2, userData="b"),
                GeoData(geom=polygon3, userData="c"),
        ]

        rdd_data = self.sc.parallelize(polygons)

        polygon_rdd = PolygonRDD(rdd_data)
        collected_data = polygon_rdd.rawSpatialRDD.collect()
        sorted_collected_data = sorted(collected_data, key=lambda x: x.userData)
        assert all([geo1 == geo2 for geo1, geo2 in zip(polygons, sorted_collected_data)])
