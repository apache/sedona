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
from shapely.geometry import Point, LineString, Polygon

from sedona.spark.core.geom.geography import Geography
from sedona.spark.sql.types import GeographyType
from sedona.spark.utils import geography_sql_serde

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


def _point_wkb(endian="<", srid=None):
    header = struct.pack(
        endian + "BI", int(endian == "<"), 1 if srid is None else 0x20000001
    )
    if srid is not None:
        header += struct.pack(endian + "i", srid)
    return header + struct.pack(endian + "dd", 1.0, 2.0)


def _srid(geometry):
    payload = wkb.dumps(geometry, big_endian=False, include_srid=True)
    geometry_type = struct.unpack_from("<I", payload, 1)[0]
    return struct.unpack_from("<i", payload, 5)[0] if geometry_type & 0x20000000 else 0


@pytest.mark.parametrize("srid", [0, 1, 4269, 4326, 2147483647])
def test_python_geography_matches_jvm_srid_prefix(srid):
    geography = Geography(wkb.loads(_point_wkb(srid=srid)))
    assert (
        GeographyType().serialize(geography) == struct.pack("<i", srid) + _point_wkb()
    )


@pytest.mark.parametrize("endian", ["<", ">"])
@pytest.mark.parametrize("srid", [0, 1, 4326])
def test_decode_jvm_geography_in_either_wkb_byte_order(endian, srid):
    value = GeographyType().deserialize(struct.pack("<i", srid) + _point_wkb(endian))
    assert isinstance(value, Geography)
    assert list(value.geometry.coords) == [(1.0, 2.0)]
    assert _srid(value.geometry) == srid


@pytest.mark.parametrize("srid", [0, 1, 4269])
def test_outer_srid_is_authoritative_for_embedded_ewkb(srid):
    value = GeographyType().deserialize(struct.pack("<i", srid) + _point_wkb(srid=4326))
    assert _srid(value.geometry) == srid


@pytest.mark.parametrize("srid", [0, 1, 4326])
@pytest.mark.parametrize("layout,coordinates", LAYOUTS)
def test_geography_uses_srid_and_iso_wkb(srid, layout, coordinates):
    iso = struct.pack("<BI", 1, 1 + layout)
    iso += struct.pack("<" + "d" * len(coordinates), *coordinates)
    source = wkb.loads(wkb.dumps(wkb.loads(iso), srid=srid, include_srid=True))
    expected = struct.pack("<i", srid) + iso
    assert GeographyType().serialize(Geography(source)) == expected
    result = GeographyType().deserialize(expected).geometry
    assert _srid(result) == srid
    assert result.wkb == source.wkb


@pytest.mark.parametrize("layout,coordinates", LAYOUTS)
def test_geography_preserves_empty_point_layout(layout, coordinates):
    iso = struct.pack("<BI", 1, 1 + layout)
    iso += bytes.fromhex("000000000000f87f") * len(coordinates)
    expected = struct.pack("<i", 4326) + iso
    result = GeographyType().deserialize(expected)
    assert result.geometry.is_empty
    assert _srid(result.geometry) == 4326
    assert GeographyType().serialize(result) == expected


@pytest.mark.parametrize("srid", [-1, -2147483648])
def test_geography_rejects_negative_srid(srid):
    with pytest.raises(ValueError, match="non-negative"):
        GeographyType().serialize(Geography(wkb.loads(_point_wkb(srid=srid))))
    with pytest.raises(ValueError, match="non-negative"):
        GeographyType().deserialize(struct.pack("<i", srid) + _point_wkb())


@pytest.mark.parametrize(
    "text",
    [
        "POINT EMPTY",
        "LINESTRING EMPTY",
        "POLYGON EMPTY",
        "POINT Z (1 2 3)",
        "POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))",
        "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))",
    ],
)
def test_geography_round_trip_preserves_coordinates_and_empty_types(text):
    source = wkt.loads(text)
    encoded = GeographyType().serialize(Geography(source))
    result = GeographyType().deserialize(encoded)
    assert result.geometry.wkb == source.wkb


@pytest.mark.skipif(
    not hasattr(shapely, "has_m") or shapely.geos_version < (3, 12, 0),
    reason="Shapely and GEOS do not expose M coordinates",
)
def test_geography_preserves_xyzm_coordinates():
    source = wkt.loads("POINT ZM (1 2 3 4)")
    result = GeographyType().deserialize(GeographyType().serialize(Geography(source)))
    assert result.geometry.wkb == source.wkb


def test_nullable_geography_transport():
    assert GeographyType().serialize(None) is None
    assert GeographyType().deserialize(None) is None


@pytest.mark.parametrize("payload", [b"", b"\x00" * 3, b"\x00" * 8])
def test_reject_truncated_geography_payload(payload):
    with pytest.raises(ValueError, match="four-byte SRID and WKB"):
        GeographyType().deserialize(payload)


@pytest.mark.parametrize("constructor", [Point, LineString, Polygon])
def test_empty_constructor_retains_declared_type_and_srid(constructor):
    geometry = constructor()
    if hasattr(shapely, "set_srid"):
        geometry = shapely.set_srid(geometry, 4326)
        previous_srid = None
    else:
        from shapely.geos import lgeos

        # Shapely 1.x constructors may share their empty GEOS object.
        previous_srid = lgeos.GEOSGetSRID(geometry._geom)
        lgeos.GEOSSetSRID(geometry._geom, 4326)
    try:
        payload = GeographyType().serialize(Geography(geometry))
        result = GeographyType().deserialize(payload).geometry
        assert result.is_empty
        assert result.geom_type == constructor.__name__
        assert _srid(result) == 4326
    finally:
        if previous_srid is not None:
            lgeos.GEOSSetSRID(geometry._geom, previous_srid)


@pytest.mark.parametrize(
    "expected_hex",
    [
        "0101000000000000000000f87f000000000000f87f",
        # Empty Z points can report has_z=False on older GEOS.
        "01e9030000000000000000f87f000000000000f87f000000000000f87f",
        "01ef03000001000000"
        "01e9030000000000000000f87f000000000000f87f000000000000f87f",
        "01ef03000002000000"
        "0101000000000000000000f03f0000000000000040"
        "01e9030000000000000000f87f000000000000f87f000000000000f87f",
        # A collection with only empty members must retain its membership.
        "0107000000010000000101000000000000000000f87f000000000000f87f",
        # A nested collection must keep the empty Point in its original position.
        "010700000002000000"
        "0101000000000000000000f03f0000000000000040"
        "0107000000010000000101000000000000000000f87f000000000000f87f",
        # MultiPoint can contain an empty member even though its constructor
        # rejects one on some Shapely versions; the public WKB reader supports it.
        "010400000002000000"
        "0101000000000000000000f87f000000000000f87f"
        "0101000000000000000000f03f0000000000000040",
    ],
)
def test_old_geos_empty_point_transport(monkeypatch, expected_hex):
    expected = bytes.fromhex(expected_hex)
    geometry = wkb.loads(expected)
    original_dumps = wkb.dumps
    original_to_wkb = getattr(shapely, "to_wkb", None)

    def unsupported(geometry):
        return geometry.geom_type in ("GeometryCollection", "MultiPoint") or (
            isinstance(geometry, Point) and geometry.is_empty
        )

    def old_writer(geometry, *args, **kwargs):
        if unsupported(geometry):
            raise AssertionError("GEOS < 3.9 cannot write empty Point WKB")
        return original_dumps(geometry, *args, **kwargs)

    # This tests the old-GEOS compatibility branch on the installed runtime;
    # it does not claim an actual GEOS < 3.9 library is installed.
    writer = geography_sql_serde.geometry_sql_serde.geometry_wkb
    monkeypatch.setattr(writer, "geos_version", (3, 8, 0), raising=False)
    monkeypatch.setattr(wkb, "dumps", old_writer)
    if original_to_wkb is not None:

        def old_to_wkb(geometry, *args, **kwargs):
            if unsupported(geometry):
                raise AssertionError("GEOS < 3.9 cannot write empty Point WKB")
            return original_to_wkb(geometry, *args, **kwargs)

        monkeypatch.setattr(shapely, "to_wkb", old_to_wkb)

    assert GeographyType().serialize(Geography(geometry)) == b"\x00" * 4 + expected


def test_old_geos_empty_point_retains_srid(monkeypatch):
    geometry = wkb.loads(
        bytes.fromhex("0101000020e6100000000000000000f87f000000000000f87f")
    )
    monkeypatch.setattr(
        geography_sql_serde.geometry_sql_serde.geometry_wkb, "geos_version", (3, 8, 0)
    )
    expected = bytes.fromhex("e61000000101000000000000000000f87f000000000000f87f")
    assert GeographyType().serialize(Geography(geometry)) == expected
