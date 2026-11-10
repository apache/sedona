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
from shapely import wkb, wkt
from shapely.geometry import LineString, Point, Polygon

from sedona.spark.sql.types import GeometryType
from sedona.spark.utils import geometry_wkb

HAS_M = hasattr(shapely, "has_m") and shapely.geos_version >= (3, 12, 0)
LAYOUTS = [
    (0, (1.0, 2.0)),
    (1000, (1.0, 2.0, 3.0)),
    pytest.param(
        2000,
        (1.0, 2.0, 3.0),
        marks=pytest.mark.skipif(not HAS_M, reason="Shapely cannot retain M"),
    ),
    pytest.param(
        3000,
        (1.0, 2.0, 3.0, 4.0),
        marks=pytest.mark.skipif(not HAS_M, reason="Shapely cannot retain M"),
    ),
]


def _with_srid(geometry, srid):
    return wkb.loads(wkb.dumps(geometry, srid=srid, include_srid=True))


def _srid(geometry):
    payload = wkb.dumps(geometry, big_endian=False, include_srid=True)
    flags = struct.unpack_from("<I", payload, 1)[0]
    return struct.unpack_from("<i", payload, 5)[0] if flags & 0x20000000 else 0


@pytest.mark.parametrize("layout,coordinates", LAYOUTS)
def test_geometry_udt_writes_independent_srid_and_iso_wkb(layout, coordinates):
    # Independent ISO type codes pin the little-endian SRID prefix and ordinates.
    iso = struct.pack("<BI", 1, 1 + layout)
    iso += struct.pack("<" + "d" * len(coordinates), *coordinates)
    expected = struct.pack("<i", 4326) + iso
    geometry = _with_srid(wkb.loads(iso), 4326)

    assert GeometryType().serialize(geometry) == expected


@pytest.mark.parametrize("byte_order", ["<", ">"])
@pytest.mark.parametrize("buffer_type", [bytes, bytearray, memoryview])
def test_geometry_udt_reads_external_iso_wkb_and_owns_coordinates(
    byte_order, buffer_type
):
    source = bytearray(
        struct.pack("<i", 3857)
        + struct.pack(byte_order + "BI2d", byte_order == "<", 1, 3, 4)
    )
    geometry = GeometryType().deserialize(buffer_type(source))
    source[:] = b"\x00" * len(source)

    assert tuple(geometry.coords[0]) == (3.0, 4.0)
    assert GeometryType().serialize(geometry) == struct.pack("<iBI2d", 3857, 1, 1, 3, 4)


@pytest.mark.parametrize(
    "text,expected_hex",
    [
        ("POINT EMPTY", "0101000000000000000000f87f000000000000f87f"),
        (
            "POINT Z EMPTY",
            "0101000080000000000000f87f000000000000f87f000000000000f87f",
        ),
        ("LINESTRING EMPTY", "010200000000000000"),
        ("LINESTRING Z EMPTY", "010200008000000000"),
        ("POLYGON EMPTY", "010300000000000000"),
        ("POLYGON Z EMPTY", "010300008000000000"),
    ],
)
def test_geometry_udt_preserves_typed_empty_layouts(text, expected_hex):
    original = bytes.fromhex(expected_hex)
    geometry = wkt.loads(text)
    if geometry.wkb != original:
        pytest.skip("This GEOS version cannot construct the requested empty layout")

    type_code = struct.unpack_from("<I", original, 1)[0]
    iso_code = (type_code & 0xFFFF) + (1000 if type_code & 0x80000000 else 0)
    expected = struct.pack("<iBI", 0, 1, iso_code) + original[5:]
    encoded = GeometryType().serialize(geometry)
    assert encoded == expected
    actual = GeometryType().deserialize(expected)
    assert actual.is_empty
    assert actual.geom_type == geometry.geom_type
    assert actual.wkb == original


@pytest.mark.parametrize(
    "constructor,type_id", [(Point, 1), (LineString, 2), (Polygon, 3)]
)
def test_geometry_udt_preserves_empty_constructor_type(constructor, type_id):
    encoded = GeometryType().serialize(constructor())
    assert struct.unpack_from("<I", encoded, 5)[0] == type_id
    actual = wkb.loads(encoded[4:])
    assert actual.is_empty
    assert actual.geom_type == constructor.__name__


@pytest.mark.parametrize(
    "constructor,type_id", [(Point, 1), (LineString, 2), (Polygon, 3)]
)
def test_geometry_udt_preserves_srid_when_restoring_empty_constructor_type(
    constructor, type_id
):
    geometry = constructor()
    if hasattr(shapely, "set_srid"):
        geometry = shapely.set_srid(geometry, 4326)
    else:
        # Shapely 1.x has no public setter. This keeps the empty constructor's
        # internal GeometryCollection so the production reconstruction is used.
        from shapely.geos import lgeos

        original_srid = lgeos.GEOSGetSRID(geometry._geom)
        lgeos.GEOSSetSRID(geometry._geom, 4326)

    expected = struct.pack("<iBI", 4326, 1, type_id)
    expected += (
        bytes.fromhex("000000000000f87f000000000000f87f")
        if type_id == 1
        else b"\x00\x00\x00\x00"
    )
    try:
        encoded = GeometryType().serialize(geometry)
    finally:
        if not hasattr(shapely, "set_srid"):
            # Empty constructors can share a GEOS object on Shapely 1.x.
            lgeos.GEOSSetSRID(geometry._geom, original_srid)

    assert encoded == expected
    actual = wkb.loads(encoded[4:])
    assert actual.is_empty
    assert actual.geom_type == constructor.__name__


def test_geometry_udt_preserves_independent_collection_member_layouts():
    # XY point, XYZ point, and XYZ empty line in one collection.
    original = bytes.fromhex(
        "010700008003000000"
        "0101000000000000000000f03f0000000000000040"
        "0101000080000000000000084000000000000010400000000000001440"
        "010200008000000000"
    )
    geometry = wkb.loads(original)
    if geometry.wkb != original:
        pytest.skip("This GEOS version changes empty collection member layouts")

    expected = bytes.fromhex(
        "0000000001ef03000003000000"
        "0101000000000000000000f03f0000000000000040"
        "01e9030000000000000000084000000000000010400000000000001440"
        "01ea03000000000000"
    )
    assert GeometryType().serialize(geometry) == expected
    actual = GeometryType().deserialize(expected)
    assert [member.geom_type for member in actual.geoms] == [
        "Point",
        "Point",
        "LineString",
    ]
    assert actual.geoms[2].is_empty
    assert actual.wkb == original


def test_geometry_udt_preserves_null():
    assert GeometryType().serialize(None) is None
    assert GeometryType().deserialize(None) is None


def test_empty_point_uses_standard_wkb_when_geos_cannot_write_it(monkeypatch):
    def unsupported_writer(*args, **kwargs):
        raise AssertionError("GEOS < 3.9 cannot write empty points")

    monkeypatch.setattr(geometry_wkb, "geos_version", (3, 8, 0), raising=False)
    monkeypatch.setattr(geometry_wkb.wkb, "dumps", unsupported_writer)
    if hasattr(shapely, "to_wkb"):
        monkeypatch.setattr(shapely, "to_wkb", unsupported_writer)

    assert GeometryType().serialize(Point()) == bytes.fromhex(
        "000000000101000000000000000000f87f000000000000f87f"
    )


@pytest.mark.parametrize(
    "expected_hex",
    [
        # A nonempty collection with an empty Point and an XY Point.
        "010700000002000000"
        "0101000000000000000000f87f000000000000f87f"
        "0101000000000000000000f03f0000000000000040",
        # Shapely 1.x hides all-empty collection members from .geoms.
        "010700000002000000"
        "0101000000000000000000f87f000000000000f87f"
        "010200000000000000",
        # Nested empty Point.
        "010700000001000000010700000001000000"
        "0101000000000000000000f87f000000000000f87f",
        # MultiPoint retains the empty member's position.
        "010400000002000000"
        "0101000000000000000000f87f000000000000f87f"
        "0101000000000000000000f03f0000000000000040",
    ],
)
def test_old_geos_collection_fallback_retains_empty_members(monkeypatch, expected_hex):
    expected = bytes.fromhex(expected_hex)
    geometry = wkb.loads(expected)
    original_dumps = wkb.dumps
    original_to_wkb = getattr(shapely, "to_wkb", None)

    def unsupported_collection_writer(geometry, *args, **kwargs):
        if geometry.geom_type in ("GeometryCollection", "MultiPoint"):
            raise AssertionError("Old GEOS cannot write collections with empty Points")
        return original_dumps(geometry, *args, **kwargs)

    monkeypatch.setattr(geometry_wkb, "geos_version", (3, 8, 0))
    monkeypatch.setattr(wkb, "dumps", unsupported_collection_writer)
    if original_to_wkb is not None:

        def unsupported_to_wkb(geometry, *args, **kwargs):
            if geometry.geom_type in ("GeometryCollection", "MultiPoint"):
                raise AssertionError(
                    "Old GEOS cannot write collections with empty Points"
                )
            return original_to_wkb(geometry, *args, **kwargs)

        monkeypatch.setattr(shapely, "to_wkb", unsupported_to_wkb)

    assert GeometryType().serialize(geometry) == b"\x00" * 4 + expected


def test_old_geos_empty_point_fallback_preserves_srid(monkeypatch):
    expected = bytes.fromhex("0101000020e6100000000000000000f87f000000000000f87f")
    geometry = wkb.loads(expected)
    monkeypatch.setattr(geometry_wkb, "geos_version", (3, 8, 0))

    assert GeometryType().serialize(geometry) == bytes.fromhex(
        "e61000000101000000000000000000f87f000000000000f87f"
    )


@pytest.mark.parametrize("srid", [0, 4326, -1, -2147483648, 2147483647])
@pytest.mark.parametrize("byte_order", ["<", ">"])
def test_geometry_udt_preserves_full_signed_srid(srid, byte_order):
    payload = struct.pack("<i", srid) + struct.pack(
        byte_order + "BI2d", byte_order == "<", 1, 1, 2
    )
    geometry = GeometryType().deserialize(payload)
    assert _srid(geometry) == srid
    assert GeometryType().serialize(geometry) == struct.pack("<iBI2d", srid, 1, 1, 1, 2)


@pytest.mark.parametrize("length", range(9))
def test_geometry_udt_rejects_truncated_framing(length):
    with pytest.raises(ValueError, match="four-byte SRID and WKB"):
        GeometryType().deserialize(b"\x00" * length)


def test_geometry_udt_rejects_invalid_wkb_byte_order():
    with pytest.raises(ValueError, match="byte order"):
        GeometryType().deserialize(struct.pack("<iBI2d", 4326, 2, 1, 1, 2))


@pytest.mark.parametrize(
    "text",
    [
        "POINT Z EMPTY",
        "LINESTRING Z EMPTY",
        "POLYGON Z EMPTY",
        "MULTILINESTRING Z ((0 0 1, 1 1 2))",
        "MULTIPOLYGON Z (((0 0 1, 1 0 2, 0 1 3, 0 0 1)))",
        "GEOMETRYCOLLECTION Z (POINT Z (1 2 3), GEOMETRYCOLLECTION Z (LINESTRING Z EMPTY))",
    ],
)
def test_legacy_writer_iso_headers_match_modern_writer(monkeypatch, text):
    source = _with_srid(wkt.loads(text), 4326)
    expected = GeometryType().serialize(source)
    # Exercise the header-only fallback using the existing public EWKB writer.
    # This is not a claim of running an actual GEOS < 3.10 library.
    monkeypatch.setattr(geometry_wkb, "geos_version", (3, 9, 0))
    assert GeometryType().serialize(source) == expected
    restored = GeometryType().deserialize(expected)
    assert _srid(restored) == 4326
    assert restored.wkb == source.wkb


@pytest.mark.parametrize("type_code", [0, 8, 4001, 1008])
def test_geometry_udt_does_not_normalize_invalid_iso_types(type_code):
    with pytest.raises(ValueError, match="Invalid geometry WKB type"):
        GeometryType().deserialize(struct.pack("<iBI2d", 4326, 1, type_code, 1, 2))


@pytest.mark.skipif(not HAS_M, reason="Shapely cannot retain M")
@pytest.mark.parametrize(
    "text,iso_hex",
    [
        ("POINT M EMPTY", "01d1070000" + "000000000000f87f" * 3),
        ("POINT ZM EMPTY", "01b90b0000" + "000000000000f87f" * 4),
        ("LINESTRING M EMPTY", "01d207000000000000"),
        ("LINESTRING ZM EMPTY", "01ba0b000000000000"),
        ("POLYGON M EMPTY", "01d307000000000000"),
        ("POLYGON ZM EMPTY", "01bb0b000000000000"),
    ],
)
def test_geometry_udt_retains_measured_empty_layouts(text, iso_hex):
    expected = struct.pack("<i", 4326) + bytes.fromhex(iso_hex)
    geometry = _with_srid(wkt.loads(text), 4326)
    assert GeometryType().serialize(geometry) == expected
    restored = GeometryType().deserialize(expected)
    assert _srid(restored) == 4326
    assert restored.wkb == geometry.wkb
