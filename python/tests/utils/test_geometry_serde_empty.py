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

import struct

import pytest
import shapely
from shapely.geometry import LineString, Point, Polygon
from shapely.wkt import loads as wkt_loads

try:
    from shapely import geos_version
except ImportError:
    from shapely.geos import geos_version

from sedona.spark.utils import geometry_serde, geometry_serde_general

EMPTY_PRIMITIVES = [("POINT", 1), ("LINESTRING", 2), ("POLYGON", 3)]
EMPTY_LAYOUTS = [("", 1), (" Z", 2)]
COLLECTIONS = [
    "GEOMETRYCOLLECTION (POINT (1 2), POINT Z EMPTY, LINESTRING Z EMPTY, "
    "POLYGON Z EMPTY)",
    "GEOMETRYCOLLECTION (POINT Z (1 2 3), POINT EMPTY, LINESTRING Z EMPTY, "
    "POLYGON EMPTY)",
    "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (1 2), POINT Z EMPTY, "
    "LINESTRING Z EMPTY, POLYGON Z EMPTY))",
    pytest.param(
        "GEOMETRYCOLLECTION (POINT Z EMPTY, LINESTRING Z EMPTY, POLYGON Z EMPTY)",
        marks=pytest.mark.skipif(
            shapely.__version__ < "2",
            reason="Shapely 1.x hides members of all-empty collections from geoms",
        ),
    ),
]


def _require_empty_layout(geometry, coord_type):
    if geometry.geom_type == "Point" and geos_version < (3, 9):
        if coord_type == 2:
            pytest.skip("Legacy empty Points use XY without WKB support")
        return

    # Some older WKT readers change the requested empty layout before serde runs.
    # Check the source geometry so the fixed header assertions remain meaningful.
    source_wkb = geometry.wkb
    byte_order = "<I" if source_wkb[0] else ">I"
    source_type = struct.unpack_from(byte_order, source_wkb, 1)[0]
    source_coord_type = 2 if source_type & 0x80000000 else 1
    if source_coord_type != coord_type:
        pytest.skip("GEOS WKT reader cannot construct the requested empty layout")


def _assert_geometry_equal(actual, expected):
    assert actual.geom_type == expected.geom_type
    if geos_version < (3, 9):
        if expected.geom_type == "GeometryCollection":
            # GEOS < 3.9 also cannot write collections containing empty Points.
            assert len(actual.geoms) == len(expected.geoms)
            for actual_member, expected_member in zip(actual.geoms, expected.geoms):
                _assert_geometry_equal(actual_member, expected_member)
            return
        if expected.geom_type == "Point" and expected.is_empty:
            assert actual.is_empty
            assert geometry_serde_general.serialize(actual) == struct.pack(
                "BBBBi", 0x12, 0, 0, 0, 0
            )
            return

    expected_wkb = expected.wkb
    assert expected_wkb  # Comparing two failed WKB writes would hide a regression.
    assert actual.wkb == expected_wkb


@pytest.mark.parametrize("wkt", [None, "POINT EMPTY", "POINT Z EMPTY"])
def test_general_empty_point_without_wkb_support(monkeypatch, wkt):
    geometry = Point() if wkt is None else wkt_loads(wkt)

    def unsupported_wkb(_geometry):
        raise AssertionError("GEOS < 3.9 cannot write empty Points as WKB")

    monkeypatch.setattr(
        geometry_serde_general, "geos_version", (3, 8, 0), raising=False
    )
    monkeypatch.setattr(geometry_serde_general, "wkb_dumps", unsupported_wkb)

    buffer = geometry_serde_general.serialize(geometry)
    actual, offset = geometry_serde_general.deserialize(buffer)

    assert buffer == struct.pack("BBBBi", 0x12, 0, 0, 0, 0)
    assert offset == len(buffer)
    assert actual.geom_type == "Point"
    assert actual.is_empty
    assert geometry_serde_general.serialize(actual) == buffer


@pytest.mark.parametrize("geometry_type,type_id", EMPTY_PRIMITIVES)
@pytest.mark.parametrize("dimension,coord_type", EMPTY_LAYOUTS)
def test_general_empty_serializer_keeps_dimension(
    geometry_type, type_id, dimension, coord_type
):
    geometry = wkt_loads(f"{geometry_type}{dimension} EMPTY")
    _require_empty_layout(geometry, coord_type)

    buffer = geometry_serde_general.serialize(geometry)

    # Check the stored layout directly: a matching decoder could hide an XY header.
    assert buffer == struct.pack(
        "BBBBi", (type_id << 4) | (coord_type << 1), 0, 0, 0, 0
    )


@pytest.mark.parametrize("geometry_type,type_id", EMPTY_PRIMITIVES)
@pytest.mark.parametrize("dimension,coord_type", EMPTY_LAYOUTS)
def test_general_empty_deserializer_keeps_stored_dimension(
    geometry_type, type_id, dimension, coord_type
):
    # Build the eight-byte internal header independently of the Python serializer,
    # as it can also come from the JVM or the C extension.
    buffer = struct.pack("BBBBi", (type_id << 4) | (coord_type << 1), 0, 0, 0, 0)
    expected = wkt_loads(f"{geometry_type}{dimension} EMPTY")
    _require_empty_layout(expected, coord_type)

    actual, offset = geometry_serde_general.deserialize(buffer)

    assert offset == len(buffer)
    _assert_geometry_equal(actual, expected)


@pytest.mark.parametrize(
    "constructor,type_id", [(Point, 1), (LineString, 2), (Polygon, 3)]
)
def test_general_empty_constructor_remains_xy(constructor, type_id):
    buffer = geometry_serde_general.serialize(constructor())
    actual, offset = geometry_serde_general.deserialize(buffer)

    assert buffer == struct.pack("BBBBi", (type_id << 4) | 2, 0, 0, 0, 0)
    assert offset == len(buffer)
    assert actual.is_empty


@pytest.mark.parametrize("wkt", COLLECTIONS)
def test_general_empty_collection_members_keep_dimension(wkt):
    geometry = wkt_loads(wkt)

    buffer = geometry_serde_general.serialize(geometry)
    actual, offset = geometry_serde_general.deserialize(buffer)

    assert offset == len(buffer)
    _assert_geometry_equal(actual, geometry)


@pytest.mark.skipif(
    not geometry_serde.speedup_enabled, reason="C extension is unavailable"
)
@pytest.mark.parametrize("c_serializes", [False, True])
@pytest.mark.parametrize(
    "wkt",
    [
        f"{geometry_type}{dimension} EMPTY"
        for geometry_type, _ in EMPTY_PRIMITIVES
        for dimension, _ in EMPTY_LAYOUTS
    ]
    + COLLECTIONS,
)
def test_empty_dimensions_cross_decoder_roundtrip(wkt, c_serializes):
    geometry = wkt_loads(wkt)
    serializer = geometry_serde if c_serializes else geometry_serde_general
    deserializer = geometry_serde_general if c_serializes else geometry_serde

    buffer = serializer.serialize(geometry)
    actual, offset = deserializer.deserialize(buffer)

    assert offset == len(buffer)
    _assert_geometry_equal(actual, geometry)


@pytest.mark.parametrize("geometry_type,type_id", EMPTY_PRIMITIVES)
@pytest.mark.parametrize("coord_type", [3, 4])
def test_general_empty_deserializer_still_rejects_m(geometry_type, type_id, coord_type):
    buffer = struct.pack("BBBBi", (type_id << 4) | (coord_type << 1), 0, 0, 0, 0)

    with pytest.raises(ValueError, match="requires geomserde_speedup"):
        geometry_serde_general.deserialize(buffer)


@pytest.mark.skipif(
    shapely.__version__ < "2.1"
    or getattr(shapely, "geos_version", (0, 0, 0)) < (3, 12, 0),
    reason="M coordinates require Shapely 2.1 and GEOS 3.12 or newer",
)
@pytest.mark.parametrize("geometry_type,type_id", EMPTY_PRIMITIVES)
@pytest.mark.parametrize("dimension", ["M", "ZM"])
def test_general_empty_serializer_still_rejects_m(geometry_type, type_id, dimension):
    geometry = wkt_loads(f"{geometry_type} {dimension} EMPTY")

    with pytest.raises(ValueError, match="requires geomserde_speedup"):
        geometry_serde_general.serialize(geometry)
