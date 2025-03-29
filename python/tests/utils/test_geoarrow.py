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

import json

import pyarrow as pa
import pyproj
import pytest
from tests.test_base import TestBase
from pyspark.sql.types import StringType, StructType
from sedona.utils.geoarrow import (
    dataframe_to_arrow,
    unique_srid_from_ewkb,
    wrap_geoarrow_field,
    wrap_geoarrow_extension,
)


class TestGeoArrow(TestBase):
    def test_to_geoarrow_no_geometry(self):
        schema = StructType().add("wkt", StringType())
        wkt_df = TestGeoArrow.spark.createDataFrame(zip(TEST_WKT), schema)
        wkt_table = dataframe_to_arrow(wkt_df)
        assert wkt_table == pa.table({"wkt": TEST_WKT})

    def test_to_geoarrow_zero_rows(self):
        schema = StructType().add("wkt", StringType())
        wkt_df = TestGeoArrow.spark.createDataFrame(zip(TEST_WKT), schema).limit(0)
        wkt_table = dataframe_to_arrow(wkt_df)
        assert wkt_table == pa.table({"wkt": pa.array([], pa.utf8())})

    def test_to_geoarrow_with_geometry(self):
        schema = StructType().add("wkt", StringType())
        wkt_df = TestGeoArrow.spark.createDataFrame(zip(TEST_WKT), schema)
        geo_df = wkt_df.selectExpr("wkt", "ST_GeomFromText(wkt) AS geom")

        geo_table = dataframe_to_arrow(geo_df)
        assert geo_table.column_names == ["wkt", "geom"]

        geom = geo_table["geom"]
        if isinstance(geom.type, pa.ExtensionType):
            assert geom.type.extension_name == "geoarrow.wkb"
            assert geom.type.crs is None
        else:
            field = geo_table.field("geom")
            assert field.metadata is not None
            assert b"ARROW:extension:name" in field.metadata
            assert field.metadata[b"ARROW:extension:name"] == b"geoarrow.wkb"
            assert field.metadata[b"ARROW:extension:metadata"] == b"{}"

    def test_to_geoarrow_with_geometry_with_srid(self):
        schema = StructType().add("wkt", StringType())
        wkt_df = TestGeoArrow.spark.createDataFrame(zip(TEST_WKT), schema)
        geo_df = wkt_df.selectExpr("ST_SetSRID(ST_GeomFromText(wkt), 4326) AS geom")

        geo_table = dataframe_to_arrow(geo_df)
        geom = geo_table["geom"]
        if isinstance(geom.type, pa.ExtensionType):
            assert geom.type.extension_name == "geoarrow.wkb"
            # CRS handling in geoarrow-types was updated in 0.2, but this should work for both
            assert "EPSG:4326" in repr(geom.type.crs)
        else:
            field = geo_table.field("geom")
            assert field.metadata is not None
            assert b"ARROW:extension:name" in field.metadata
            assert field.metadata[b"ARROW:extension:name"] == b"geoarrow.wkb"

            metadata = json.loads(field.metadata[b"ARROW:extension:metadata"])
            assert "crs" in metadata
            assert "id" in metadata["crs"]
            assert metadata["crs"]["id"] == {"authority": "EPSG", "code": 4326}

    def test_wrap_field(self):
        col_empty = pa.array([], pa.binary())
        field = pa.field("foofy", col_empty.type)

        # With pyproj object crs override
        wrapped = wrap_geoarrow_field(field, col_empty, pyproj.CRS("OGC:CRS84"))
        assert wrapped.metadata[b"ARROW:extension:name"] == b"geoarrow.wkb"
        assert b"WGS 84 (CRS84)" in wrapped.metadata[b"ARROW:extension:metadata"]

        # With arbitrary string override
        wrapped = wrap_geoarrow_field(field, col_empty, "OGC:CRS84")
        assert wrapped.metadata[b"ARROW:extension:name"] == b"geoarrow.wkb"
        assert b"WGS 84 (CRS84)" in wrapped.metadata[b"ARROW:extension:metadata"]

        # With no output CRS
        wrapped = wrap_geoarrow_field(field, col_empty, None)
        assert wrapped.metadata[b"ARROW:extension:name"] == b"geoarrow.wkb"
        assert wrapped.metadata[b"ARROW:extension:metadata"] == b"{}"

        # With inferred crs
        col = pa.array(TEST_WKB["ewkb_srid_little_endian"])
        wrapped = wrap_geoarrow_field(field, col, None)
        assert wrapped.metadata[b"ARROW:extension:name"] == b"geoarrow.wkb"
        assert b"WGS 84" in wrapped.metadata[b"ARROW:extension:metadata"]

    def test_wrap_extension(self):
        gat = pytest.importorskip("geoarrow.types")

        col_empty = pa.array([], pa.binary())
        spec = gat.wkb()

        # With pyproj object crs override
        wrapped = wrap_geoarrow_extension(col_empty, spec, pyproj.CRS("OGC:CRS84"))
        assert wrapped.type.encoding == gat.Encoding.WKB
        assert "WGS 84 (CRS84)" in wrapped.type.crs.to_json()

        # With no output CRS
        wrapped = wrap_geoarrow_extension(col_empty, spec, None)
        assert wrapped.type.encoding == gat.Encoding.WKB
        assert wrapped.type.crs is None

        # With arbitrary string override
        wrapped = wrap_geoarrow_extension(col_empty, spec, "OGC:CRS84")
        assert wrapped.type.encoding == gat.Encoding.WKB
        assert "WGS 84 (CRS84)" in wrapped.type.crs.to_json()

        # With inferred crs
        col = pa.array(TEST_WKB["ewkb_srid_little_endian"])
        wrapped = wrap_geoarrow_extension(col, spec, None)
        assert wrapped.type.encoding == gat.Encoding.WKB
        assert "WGS 84" in wrapped.type.crs.to_json()

    def test_unique_srid(self):
        # Zero size should return None
        assert unique_srid_from_ewkb(pa.array([], pa.binary())) is None

        # EWKB with no SRID should return None here
        assert unique_srid_from_ewkb(pa.array(TEST_WKB["ewkb_little_endian"])) is None
        assert unique_srid_from_ewkb(pa.array(TEST_WKB["ewkb_big_endian"])) is None

        # EWKB with SRID
        assert (
            unique_srid_from_ewkb(pa.array(TEST_WKB["ewkb_srid_little_endian"])) == 4326
        )
        assert unique_srid_from_ewkb(pa.array(TEST_WKB["ewkb_srid_big_endian"])) == 4326

        # In the presence of geometries with SRID and without SRID, the geometries
        # without SRID are not counted
        assert (
            unique_srid_from_ewkb(
                pa.array(
                    TEST_WKB["ewkb_little_endian"] + TEST_WKB["ewkb_srid_little_endian"]
                )
            )
            == 4326
        )

        # If there is more than one SRID present, we return None
        # "SRID=1234;POINT (10 20)"
        ewkb_other_srid = b"\x01\x01\x00\x00 \xd2\x04\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@"
        assert (
            unique_srid_from_ewkb(
                pa.array(TEST_WKB["ewkb_srid_little_endian"] + [ewkb_other_srid])
            )
            is None
        )

        # Mixed endian (unlikely) should return None
        assert (
            unique_srid_from_ewkb(
                pa.array(
                    TEST_WKB["ewkb_srid_big_endian"]
                    + TEST_WKB["ewkb_srid_little_endian"]
                )
            )
            is None
        )


TEST_WKT = [
    "POINT (10 20)",
    "POINT (10 20 30)",
    "LINESTRING (10 20, 30 40)",
    "LINESTRING (10 20 30, 40 50 60)",
    "POLYGON ((10 10, 20 20, 20 10, 10 10))",
    "POLYGON ((10 10 10, 20 20 10, 20 10 10, 10 10 10))",
    "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))",
]

# A little tricky to generate EWKB test data because shapely/GEOS
# does not support M values. In R we have two options that can do this
# (sf and wk), so the test data was generated there. We need test data
# with all dimensions because this affects the high bytes used to detect
# the presence of an SRID; we need both endians because this affects the
# decoding of the high bits and the SRID value.
#
# ewkt <- c(
#   "POINT (10 20)",
#   "POINT Z (10 20 2)",
#   "POINT M (10 20 2)",
#   "POINT ZM (10 20 2 3)",
#   "POINT (10 20)",
#   "LINESTRING (10 20, 30 40)",
#   "POLYGON ((10 10, 20 20, 20 10, 10 10))",
#   "MULTIPOINT ((10 20))",
#   "MULTILINESTRING ((10 20, 30 40))",
#   "MULTIPOLYGON (((10 10, 20 20, 20 10, 10 10)))",
#   "GEOMETRYCOLLECTION (POINT (10 20))"
# )
#
# ewkt_srid <- paste0("SRID=4326;", ewkt)
#
# ewkb_test <- list(
#   ewkb_little_endian = wk::wkt_translate_wkb(ewkt, endian = 1),
#   ewkb_big_endian = wk::wkt_translate_wkb(ewkt, endian = 0),
#   ewkb_srid_little_endian = wk::wkt_translate_wkb(ewkt_srid, endian = 1),
#   ewkb_srid_big_endian = wk::wkt_translate_wkb(ewkt_srid, endian = 0)
# )
#
# # Generate a version of this we can paste into Python, with a bit of
# # indirection because reticulate converts raw vectors as bytearray
# py <- reticulate::py
# py$ekwb <- ewkb_test
# py_ewkb <- reticulate::py_eval(
#   "{k: [bytes(i) for i in v] for k, v in ekwb.items()}",
#   convert = FALSE
# )
# print(py_ewkb)
TEST_WKB = {
    "ewkb_little_endian": [
        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@",
        b"\x01\x01\x00\x00\x80\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x00\x00@",
        b"\x01\x01\x00\x00@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x00\x00@",
        b"\x01\x01\x00\x00\xc0\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x08@",
        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@",
        b"\x01\x02\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x00>@\x00\x00\x00\x00\x00\x00D@",
        b"\x01\x03\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@",
        b"\x01\x04\x00\x00\x00\x01\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@",
        b"\x01\x05\x00\x00\x00\x01\x00\x00\x00\x01\x02\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x00>@\x00\x00\x00\x00\x00\x00D@",
        b"\x01\x06\x00\x00\x00\x01\x00\x00\x00\x01\x03\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@",
        b"\x01\x07\x00\x00\x00\x01\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@",
    ],
    "ewkb_big_endian": [
        b"\x00\x00\x00\x00\x01@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00",
        b"\x00\x80\x00\x00\x01@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00",
        b"\x00@\x00\x00\x01@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00",
        b"\x00\xc0\x00\x00\x01@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x08\x00\x00\x00\x00\x00\x00",
        b"\x00\x00\x00\x00\x01@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00",
        b"\x00\x00\x00\x00\x02\x00\x00\x00\x02@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@>\x00\x00\x00\x00\x00\x00@D\x00\x00\x00\x00\x00\x00",
        b"\x00\x00\x00\x00\x03\x00\x00\x00\x01\x00\x00\x00\x04@$\x00\x00\x00\x00\x00\x00@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@$\x00\x00\x00\x00\x00\x00@$\x00\x00\x00\x00\x00\x00@$\x00\x00\x00\x00\x00\x00",
        b"\x00\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x00\x01@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00",
        b"\x00\x00\x00\x00\x05\x00\x00\x00\x01\x00\x00\x00\x00\x02\x00\x00\x00\x02@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@>\x00\x00\x00\x00\x00\x00@D\x00\x00\x00\x00\x00\x00",
        b"\x00\x00\x00\x00\x06\x00\x00\x00\x01\x00\x00\x00\x00\x03\x00\x00\x00\x01\x00\x00\x00\x04@$\x00\x00\x00\x00\x00\x00@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@$\x00\x00\x00\x00\x00\x00@$\x00\x00\x00\x00\x00\x00@$\x00\x00\x00\x00\x00\x00",
        b"\x00\x00\x00\x00\x07\x00\x00\x00\x01\x00\x00\x00\x00\x01@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00",
    ],
    "ewkb_srid_little_endian": [
        b"\x01\x01\x00\x00 \xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@",
        b"\x01\x01\x00\x00\xa0\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x00\x00@",
        b"\x01\x01\x00\x00`\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x00\x00@",
        b"\x01\x01\x00\x00\xe0\xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x08@",
        b"\x01\x01\x00\x00 \xe6\x10\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@",
        b"\x01\x02\x00\x00 \xe6\x10\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x00>@\x00\x00\x00\x00\x00\x00D@",
        b"\x01\x03\x00\x00 \xe6\x10\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@",
        b"\x01\x04\x00\x00 \xe6\x10\x00\x00\x01\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@",
        b"\x01\x05\x00\x00 \xe6\x10\x00\x00\x01\x00\x00\x00\x01\x02\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x00>@\x00\x00\x00\x00\x00\x00D@",
        b"\x01\x06\x00\x00 \xe6\x10\x00\x00\x01\x00\x00\x00\x01\x03\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x004@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x00$@",
        b"\x01\x07\x00\x00 \xe6\x10\x00\x00\x01\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00$@\x00\x00\x00\x00\x00\x004@",
    ],
    "ewkb_srid_big_endian": [
        b"\x00 \x00\x00\x01\x00\x00\x10\xe6@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00",
        b"\x00\xa0\x00\x00\x01\x00\x00\x10\xe6@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00",
        b"\x00`\x00\x00\x01\x00\x00\x10\xe6@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00",
        b"\x00\xe0\x00\x00\x01\x00\x00\x10\xe6@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@\x08\x00\x00\x00\x00\x00\x00",
        b"\x00 \x00\x00\x01\x00\x00\x10\xe6@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00",
        b"\x00 \x00\x00\x02\x00\x00\x10\xe6\x00\x00\x00\x02@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@>\x00\x00\x00\x00\x00\x00@D\x00\x00\x00\x00\x00\x00",
        b"\x00 \x00\x00\x03\x00\x00\x10\xe6\x00\x00\x00\x01\x00\x00\x00\x04@$\x00\x00\x00\x00\x00\x00@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@$\x00\x00\x00\x00\x00\x00@$\x00\x00\x00\x00\x00\x00@$\x00\x00\x00\x00\x00\x00",
        b"\x00 \x00\x00\x04\x00\x00\x10\xe6\x00\x00\x00\x01\x00\x00\x00\x00\x01@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00",
        b"\x00 \x00\x00\x05\x00\x00\x10\xe6\x00\x00\x00\x01\x00\x00\x00\x00\x02\x00\x00\x00\x02@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@>\x00\x00\x00\x00\x00\x00@D\x00\x00\x00\x00\x00\x00",
        b"\x00 \x00\x00\x06\x00\x00\x10\xe6\x00\x00\x00\x01\x00\x00\x00\x00\x03\x00\x00\x00\x01\x00\x00\x00\x04@$\x00\x00\x00\x00\x00\x00@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00@$\x00\x00\x00\x00\x00\x00@$\x00\x00\x00\x00\x00\x00@$\x00\x00\x00\x00\x00\x00",
        b"\x00 \x00\x00\x07\x00\x00\x10\xe6\x00\x00\x00\x01\x00\x00\x00\x00\x01@$\x00\x00\x00\x00\x00\x00@4\x00\x00\x00\x00\x00\x00",
    ],
}
