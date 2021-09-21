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

from pyspark.sql.types import IntegerType
import geopandas as gpd

from tests import tests_resource
from sedona.sql.types import GeometryType
from shapely.geometry import Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon
from pyspark.sql import types as t

from tests.test_base import TestBase


class TestsSerializers(TestBase):

    def test_point_serializer(self):
        data = [
            [1, Point(21.0, 56.0), Point(21.0, 59.0)]

        ]

        schema = t.StructType(
            [
                t.StructField("id", IntegerType(), True),
                t.StructField("geom_from", GeometryType(), True),
                t.StructField("geom_to", GeometryType(), True)
            ]
        )
        self.spark.createDataFrame(
            data,
            schema
        ).createOrReplaceTempView("points")

        distance = self.spark.sql(
            "select st_distance(geom_from, geom_to) from points"
        ).collect()[0][0]
        assert distance == 3.0

    def test_multipoint_serializer(self):

        multipoint = MultiPoint([
                [21.0, 56.0],
                [21.0, 57.0]
             ])
        data = [
            [1, multipoint]
        ]

        schema = t.StructType(
            [
                t.StructField("id", IntegerType(), True),
                t.StructField("geom", GeometryType(), True)
            ]
        )
        m_point_out = self.spark.createDataFrame(
            data,
            schema
        ).collect()[0][1]

        assert m_point_out == multipoint

    def test_linestring_serialization(self):
        linestring = LineString([(0.0, 1.0), (1, 1), (12.0, 1.0)])
        data = [
            [1, linestring]
        ]

        schema = t.StructType(
            [
                t.StructField("id", IntegerType(), True),
                t.StructField("geom", GeometryType(), True)
            ]
        )

        self.spark.createDataFrame(
            data,
            schema
        ).createOrReplaceTempView("line")

        length = self.spark.sql("select st_length(geom) from line").collect()[0][0]
        assert length == 12.0

    def test_multilinestring_serialization(self):
        multilinestring = MultiLineString([[[0, 1], [1, 1]], [[2, 2], [3, 2]]])
        data = [
            [1, multilinestring]
        ]

        schema = t.StructType(
            [
                t.StructField("id", IntegerType(), True),
                t.StructField("geom", GeometryType(), True)
            ]
        )

        self.spark.createDataFrame(
            data,
            schema
        ).createOrReplaceTempView("multilinestring")

        length = self.spark.sql("select st_length(geom) from multilinestring").collect()[0][0]
        assert length == 2.0

    def test_polygon_serialization(self):
        ext = [(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]
        int = [(1, 1), (1, 1.5), (1.5, 1.5), (1.5, 1), (1, 1)]

        polygon = Polygon(ext, [int])

        data = [
            [1, polygon]
        ]

        schema = t.StructType(
            [
                t.StructField("id", IntegerType(), True),
                t.StructField("geom", GeometryType(), True)
            ]
        )

        self.spark.createDataFrame(
            data,
            schema
        ).createOrReplaceTempView("polygon")

        length = self.spark.sql("select st_area(geom) from polygon").collect()[0][0]
        assert length == 3.75

    def test_geopandas_convertion(self):
        gdf = gpd.read_file(os.path.join(tests_resource, "shapefiles/gis_osm_pois_free_1/"))
        print(self.spark.createDataFrame(
            gdf
        ).toPandas())

    def test_multipolygon_serialization(self):
        exterior = [(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]
        interior = [(1, 1), (1, 1.5), (1.5, 1.5), (1.5, 1), (1, 1)]

        polygons = [
            Polygon(exterior, [interior]),
            Polygon([[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]])
        ]
        multipolygon = MultiPolygon(polygons)

        data = [
            [1, multipolygon]
        ]

        schema = t.StructType(
            [
                t.StructField("id", IntegerType(), True),
                t.StructField("geom", GeometryType(), True)
            ]
        )
        self.spark.createDataFrame(
            data,
            schema
        ).show(1, False)
        self.spark.createDataFrame(
            data,
            schema
        ).createOrReplaceTempView("polygon")
        length = self.spark.sql("select st_area(geom) from polygon").collect()[0][0]
        assert length == 4.75
