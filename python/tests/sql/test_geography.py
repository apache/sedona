# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pyspark.sql.functions import col, lit
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf
from sedona.spark.sql import st_predicates as stp
from tests.test_base import TestBase


class TestGeographyConstructorsDataFrameAPI(TestBase):
    """Exercise every ST_Geog* constructor through its typed Python wrapper."""

    def test_st_geog_from_wkt(self):
        df = self.spark.sql("SELECT 'POINT (1 2)' AS wkt").select(
            stc.ST_GeogFromWKT(col("wkt"), lit(4326)).alias("g")
        )
        ewkt = df.select(stf.ST_AsEWKT(col("g"))).first()[0]
        assert ewkt == "SRID=4326; POINT (1 2)"

    def test_st_geog_from_wkt_no_srid(self):
        df = self.spark.sql("SELECT 'POINT (1 2)' AS wkt").select(
            stc.ST_GeogFromWKT(col("wkt")).alias("g")
        )
        wkt = df.select(stf.ST_AsText(col("g"))).first()[0]
        assert wkt == "POINT (1 2)"

    def test_st_geog_from_text(self):
        df = self.spark.sql("SELECT 'POINT (3 4)' AS wkt").select(
            stc.ST_GeogFromText(col("wkt"), lit(4326)).alias("g")
        )
        ewkt = df.select(stf.ST_AsEWKT(col("g"))).first()[0]
        assert ewkt == "SRID=4326; POINT (3 4)"

    def test_st_geog_from_wkb(self):
        # WKB for POINT (10 15) in little-endian
        wkb_bytes = bytes(
            [1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 36, 64, 0, 0, 0, 0, 0, 0, 46, 64]
        )
        df = self.spark.createDataFrame([(wkb_bytes,)], ["wkb"]).select(
            stc.ST_GeogFromWKB(col("wkb")).alias("g")
        )
        wkt = df.select(stf.ST_AsText(col("g"))).first()[0]
        assert wkt == "POINT (10 15)"

    def test_st_geog_from_ewkb(self):
        # EWKB for SRID=4326; LINESTRING (-2.1 -0.4, -1.5 -0.7)
        ewkb_bytes = bytes(
            [
                1,
                2,
                0,
                0,
                32,
                230,
                16,
                0,
                0,
                2,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                132,
                214,
                0,
                192,
                0,
                0,
                0,
                0,
                128,
                181,
                214,
                191,
                0,
                0,
                0,
                96,
                225,
                239,
                247,
                191,
                0,
                0,
                0,
                128,
                7,
                93,
                229,
                191,
            ]
        )
        df = self.spark.createDataFrame([(ewkb_bytes,)], ["wkb"]).select(
            stc.ST_GeogFromEWKB(col("wkb")).alias("g")
        )
        ewkt = df.select(stf.ST_AsEWKT(col("g"))).first()[0]
        assert ewkt.startswith("SRID=4326; LINESTRING")

    def test_st_geog_from_ewkt(self):
        df = self.spark.sql("SELECT 'SRID=4269;POINT (5 6)' AS ewkt").select(
            stc.ST_GeogFromEWKT(col("ewkt")).alias("g")
        )
        ewkt = df.select(stf.ST_AsEWKT(col("g"))).first()[0]
        assert ewkt == "SRID=4269; POINT (5 6)"

    def test_st_geog_from_geohash(self):
        df = self.spark.sql("SELECT '9q9j8ue2v71y5zzy0s4q' AS geohash").select(
            stc.ST_GeogFromGeoHash(col("geohash"), 4).alias("g")
        )
        wkt = df.select(stf.ST_AsText(col("g"))).first()[0]
        assert wkt.startswith("POLYGON")

    def test_st_geog_from_geohash_no_precision(self):
        df = self.spark.sql("SELECT '9q9' AS geohash").select(
            stc.ST_GeogFromGeoHash(col("geohash")).alias("g")
        )
        wkt = df.select(stf.ST_AsText(col("g"))).first()[0]
        assert wkt.startswith("POLYGON")

    def test_st_geogcoll_from_text(self):
        wkt_in = "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))"
        df = self.spark.sql(f"SELECT '{wkt_in}' AS wkt").select(
            stc.ST_GeogCollFromText(col("wkt"), lit(4326)).alias("g")
        )
        ewkt = df.select(stf.ST_AsEWKT(col("g"))).first()[0]
        assert ewkt.startswith("SRID=4326; GEOMETRYCOLLECTION")

    def test_st_geog_to_geometry(self):
        df = (
            self.spark.sql("SELECT 'POINT (7 8)' AS wkt")
            .select(stc.ST_GeogFromWKT(col("wkt"), lit(4326)).alias("g"))
            .select(stc.ST_GeogToGeometry(col("g")).alias("geom"))
        )
        wkt = df.select(stf.ST_AsText(col("geom"))).first()[0]
        assert wkt == "POINT (7 8)"

    def test_st_geom_to_geography(self):
        df = (
            self.spark.sql("SELECT 'POINT (9 10)' AS wkt")
            .select(stc.ST_GeomFromWKT(col("wkt"), lit(4326)).alias("geom"))
            .select(stc.ST_GeomToGeography(col("geom")).alias("g"))
        )
        ewkt = df.select(stf.ST_AsEWKT(col("g"))).first()[0]
        assert ewkt == "SRID=4326; POINT (9 10)"


class TestGeographyFunctionsDataFrameAPI(TestBase):
    """Exercise dual-dispatch ST functions/predicates against Geography columns
    via the typed Python DataFrame API."""

    def _geog(self, wkt, srid=4326):
        return stc.ST_GeogFromWKT(lit(wkt), lit(srid))

    def test_st_distance(self):
        df = self.spark.range(1).select(
            stf.ST_Distance(self._geog("POINT (0 0)"), self._geog("POINT (1 1)")).alias(
                "d"
            )
        )
        d = df.first()[0]
        assert 155000 < d < 160000  # ~157km on a sphere

    def test_st_length(self):
        df = self.spark.range(1).select(
            stf.ST_Length(self._geog("LINESTRING (0 0, 1 0)")).alias("l")
        )
        l = df.first()[0]
        assert 110000 < l < 112000

    def test_st_length_of_point(self):
        df = self.spark.range(1).select(
            stf.ST_Length(self._geog("POINT (1 2)")).alias("l")
        )
        assert df.first()[0] == 0.0

    def test_st_area(self):
        df = self.spark.range(1).select(
            stf.ST_Area(self._geog("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")).alias("a")
        )
        a = df.first()[0]
        # 1°×1° box near equator on R=6371008m sphere ≈ 1.2364e10 m²
        assert 1.23e10 < a < 1.24e10

    def test_st_centroid(self):
        df = self.spark.range(1).select(
            stf.ST_Centroid(self._geog("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))")).alias(
                "c"
            )
        )
        wkt = df.select(stf.ST_AsText(col("c"))).first()[0]
        assert wkt.startswith("POINT")

    def test_st_buffer(self):
        df = self.spark.range(1).select(
            stf.ST_Buffer(self._geog("POINT (0 0)"), lit(1000.0)).alias("b")
        )
        wkt = df.select(stf.ST_AsText(col("b"))).first()[0]
        assert wkt.startswith("POLYGON")

    def test_st_envelope(self):
        df = self.spark.range(1).select(
            stf.ST_Envelope(self._geog("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")).alias(
                "e"
            )
        )
        wkt = df.select(stf.ST_AsText(col("e"))).first()[0]
        assert wkt.startswith("POLYGON")

    def test_st_npoints(self):
        df = self.spark.range(1).select(
            stf.ST_NPoints(self._geog("LINESTRING (0 0, 1 1, 2 2)")).alias("n")
        )
        assert df.first()[0] == 3

    def test_st_contains(self):
        df = self.spark.range(1).select(
            stp.ST_Contains(
                self._geog("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"),
                self._geog("POINT (0.5 0.5)"),
            ).alias("r")
        )
        assert df.first()[0] is True

    def test_st_within(self):
        df = self.spark.range(1).select(
            stp.ST_Within(
                self._geog("POINT (0.5 0.5)"),
                self._geog("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"),
            ).alias("r")
        )
        assert df.first()[0] is True

    def test_st_dwithin(self):
        df = self.spark.range(1).select(
            stp.ST_DWithin(
                self._geog("POINT (0 0)"),
                self._geog("POINT (0 1)"),
                lit(200000.0),
            ).alias("r")
        )
        assert df.first()[0] is True

    def test_st_equals(self):
        df = self.spark.range(1).select(
            stp.ST_Equals(
                self._geog("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"),
                self._geog("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"),
            ).alias("r")
        )
        assert df.first()[0] is True

    def test_st_intersects(self):
        df = self.spark.range(1).select(
            stp.ST_Intersects(
                self._geog("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"),
                self._geog("POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))"),
            ).alias("r")
        )
        assert df.first()[0] is True
