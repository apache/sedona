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


@pytest.mark.parametrize("geometry_type,type_id", EMPTY_PRIMITIVES)
@pytest.mark.parametrize("dimension,coord_type", EMPTY_LAYOUTS)
def test_general_empty_serializer_keeps_dimension(
    geometry_type, type_id, dimension, coord_type
):
    geometry = wkt_loads(f"{geometry_type}{dimension} EMPTY")

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

    actual, offset = geometry_serde_general.deserialize(buffer)

    assert offset == len(buffer)
    assert actual.wkb == wkt_loads(f"{geometry_type}{dimension} EMPTY").wkb


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
    assert actual.wkb == geometry.wkb


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
    assert actual.wkb == geometry.wkb


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
