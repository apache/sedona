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
import math

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


@pytest.mark.parametrize(
    "geometry_type,type_id",
    [("MULTIPOINT", 4), ("MULTILINESTRING", 5), ("MULTIPOLYGON", 6)],
)
@pytest.mark.parametrize("dimension", ["", " Z"])
@pytest.mark.parametrize("members", ["leading", "trailing", "both", "all", "none"])
def test_general_multipart_roundtrip_keeps_empty_members(
    geometry_type, type_id, dimension, members
):
    ring = "0 0, 1 0, 1 1, 0 0" if not dimension else "0 0 2, 1 0 2, 1 1 2, 0 0 2"
    nonempty_member = {
        4: "(0 0)" if not dimension else "(0 0 2)",
        5: f"({ring})",
        6: f"(({ring}))",
    }[type_id]
    parts = {
        "leading": f"EMPTY, {nonempty_member}",
        "trailing": f"{nonempty_member}, EMPTY",
        "both": f"EMPTY, {nonempty_member}, EMPTY",
        "all": "EMPTY, EMPTY",
        "none": nonempty_member,
    }
    geometry = wkt_loads(f"{geometry_type}{dimension} ({parts[members]})")
    if members == "all" and shapely.__version__ < "2":
        pytest.skip("Shapely 1.x hides all-empty members from the serializer")
    if members == "all":
        _require_empty_layout(geometry, 2 if dimension else 1)

    buffer = geometry_serde_general.serialize(geometry)
    actual, offset = geometry_serde_general.deserialize(buffer + b"trailing bytes")

    assert offset == len(buffer)
    # Some GEOS versions construct XY empty members even inside a Z
    # multipart geometry. The internal format stores one shared coordinate layout,
    # so those empty members are normalized to that layout on reconstruction.
    z_flag = 0x80000000 if dimension else 0
    expected_wkb = struct.pack("<BII", 1, type_id | z_flag, len(geometry.geoms))
    empty_wkb = struct.pack("<BI", 1, (type_id - 3) | z_flag)
    empty_wkb += (
        struct.pack(
            "<" + "d" * (3 if dimension else 2), *([math.nan] * (3 if dimension else 2))
        )
        if type_id == 4
        else struct.pack("<I", 0)
    )
    expected_wkb += b"".join(
        empty_wkb if part.is_empty else part.wkb for part in geometry.geoms
    )
    assert actual.wkb == expected_wkb


@pytest.mark.parametrize("coord_type", [1, 2])
@pytest.mark.parametrize("empty_ring_count", [0, 1])
def test_general_multipolygon_all_empty_stored_members(coord_type, empty_ring_count):
    # The wire format shares a layout across all members. Preserve that layout
    # even when no coordinates are available to infer it from.
    structure = [2]
    for _ in range(2):
        structure.append(empty_ring_count)
        if empty_ring_count:
            structure.append(0)
    buffer = struct.pack("BBBBi", 0x60 | (coord_type << 1), 0, 0, 0, 0)
    buffer += struct.pack(f"{len(structure)}i", *structure)
    z_flag = 0x80000000 if coord_type == 2 else 0
    polygon_type = 3 | z_flag
    expected_wkb = struct.pack("<BII", 1, 6 | z_flag, 2)
    expected_wkb += struct.pack("<BII", 1, polygon_type, 0) * 2

    actual, offset = geometry_serde_general.deserialize(buffer + b"trailing bytes")

    assert offset == len(buffer)
    assert actual.wkb == expected_wkb


@pytest.mark.parametrize("type_id", [4, 5, 6])
@pytest.mark.parametrize("coord_type", [1, 2])
@pytest.mark.parametrize("num_members", [0, 2])
def test_general_multipart_stored_empty_members(type_id, coord_type, num_members):
    dimension = coord_type + 1
    num_coords = num_members if type_id == 4 else 0
    buffer = struct.pack(
        "BBBBi", (type_id << 4) | (coord_type << 1), 0, 0, 0, num_coords
    )
    if type_id == 4:
        buffer += struct.pack(
            "d" * (dimension * num_members), *([math.nan] * (dimension * num_members))
        )
    else:
        buffer += struct.pack(
            "i" * (num_members + 1), num_members, *([0] * num_members)
        )
    z_flag = 0x80000000 if coord_type == 2 else 0
    empty_wkb = struct.pack("<BI", 1, (type_id - 3) | z_flag)
    empty_wkb += (
        struct.pack("<" + "d" * dimension, *([math.nan] * dimension))
        if type_id == 4
        else struct.pack("<I", 0)
    )
    # GEOS canonicalizes zero-member multipart geometries to XY, even when
    # loading WKB with an explicit Z flag.
    collection_z_flag = z_flag if num_members else 0
    expected_wkb = (
        struct.pack("<BII", 1, type_id | collection_z_flag, num_members)
        + empty_wkb * num_members
    )

    actual, offset = geometry_serde_general.deserialize(buffer + b"trailing bytes")

    assert offset == len(buffer)
    assert actual.wkb == expected_wkb


@pytest.mark.parametrize("coordinates", [(math.nan, 1.0), (math.nan, 1.0, 2.0)])
def test_general_multipoint_preserves_partially_nan_point(coordinates):
    coord_type = len(coordinates) - 1
    buffer = struct.pack("BBBBi", 0x40 | (coord_type << 1), 0, 0, 0, 1)
    buffer += struct.pack("d" * len(coordinates), *coordinates)
    z_flag = 0x80000000 if coord_type == 2 else 0
    expected_wkb = struct.pack("<BII", 1, 4 | z_flag, 1)
    expected_wkb += struct.pack("<BI", 1, 1 | z_flag)
    expected_wkb += struct.pack("<" + "d" * len(coordinates), *coordinates)

    actual, offset = geometry_serde_general.deserialize(buffer)

    assert offset == len(buffer)
    assert actual.wkb == expected_wkb
