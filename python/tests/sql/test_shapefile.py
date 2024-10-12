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

import datetime
import os.path

import pytest
from tests.test_base import TestBase
from tests.tools import tests_resource


class TestShapefile(TestBase):
    def test_read_simple(self):
        input_location = os.path.join(tests_resource, "shapefiles/polygon")
        df = self.spark.read.format("shapefile").load(input_location)
        assert df.count() == 10000
        rows = df.take(100)
        for row in rows:
            assert len(row) == 1
            assert row["geometry"].geom_type in ("Polygon", "MultiPolygon")

    def test_read_osm_pois(self):
        input_location = os.path.join(
            tests_resource, "shapefiles/gis_osm_pois_free_1/gis_osm_pois_free_1.shp"
        )
        df = self.spark.read.format("shapefile").load(input_location)
        assert df.count() == 12873
        rows = df.take(100)
        for row in rows:
            assert len(row) == 5
            assert row["geometry"].geom_type == "Point"
            assert isinstance(row["osm_id"], str)
            assert isinstance(row["fclass"], str)
            assert isinstance(row["name"], str)
            assert isinstance(row["code"], int)

    def test_customize_geom_and_key_columns(self):
        input_location = os.path.join(tests_resource, "shapefiles/gis_osm_pois_free_1")
        df = (
            self.spark.read.format("shapefile")
            .option("geometry.name", "geom")
            .option("key.name", "fid")
            .load(input_location)
        )
        assert df.count() == 12873
        rows = df.take(100)
        for row in rows:
            assert len(row) == 6
            assert row["geom"].geom_type == "Point"
            assert isinstance(row["fid"], int)
            assert isinstance(row["osm_id"], str)
            assert isinstance(row["fclass"], str)
            assert isinstance(row["name"], str)
            assert isinstance(row["code"], int)

    def test_read_multiple_shapefiles(self):
        input_location = os.path.join(tests_resource, "shapefiles/datatypes")
        df = self.spark.read.format("shapefile").load(input_location)
        rows = df.collect()
        assert len(rows) == 9
        for row in rows:
            id = row["id"]
            assert row["aInt"] == id
            if id is not None:
                assert row["aUnicode"] == "测试" + str(id)
                if id < 10:
                    assert row["aDecimal"] * 10 == id * 10 + id
                    assert row["aDecimal2"] is None
                    assert row["aDate"] == datetime.date(2020 + id, id, id)
                else:
                    assert row["aDecimal"] is None
                    assert row["aDecimal2"] * 100 == id * 100 + id
                    assert row["aDate"] is None
            else:
                assert row["aUnicode"] == ""
                assert row["aDecimal"] is None
                assert row["aDecimal2"] is None
                assert row["aDate"] is None
