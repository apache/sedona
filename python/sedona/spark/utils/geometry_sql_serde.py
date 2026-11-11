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

"""SQL geometry transport: little-endian signed SRID followed by ISO WKB."""

import struct

import shapely

from sedona.spark.utils import geometry_wkb


def serialize_geometry(geometry):
    """Encode a SQL geometry, retaining its SRID and supported coordinate dimensions."""
    if geometry is None:
        return None
    srid = geometry_wkb._get_srid(geometry)
    if hasattr(shapely, "to_wkb") and geometry_wkb.geos_version >= (3, 10, 0):
        payload = shapely.to_wkb(
            geometry,
            byte_order=1,
            include_srid=False,
            flavor="iso",
            output_dimension=(
                4
                if hasattr(shapely, "has_m") and shapely.geos_version >= (3, 12, 0)
                else 3
            ),
        )
    else:
        # Keep the shared writer's older-GEOS empty and Shapely 1.x type fixes.
        # Only WKB headers are adapted; ordinates remain the public writer's bytes.
        payload = _iso_headers(geometry_wkb.dumps(geometry))
    return struct.pack("<i", srid) + payload


def deserialize_geometry(value):
    """Decode the SRID-prefixed WKB without retaining a mutable input buffer."""
    if value is None:
        return None
    value = bytes(value)
    if len(value) < 9:
        raise ValueError("Geometry payload must contain a four-byte SRID and WKB")
    srid = struct.unpack_from("<i", value)[0]
    payload = value[4:]
    if payload[0] not in (0, 1):
        raise ValueError("Invalid geometry WKB byte order")
    endian = "<" if payload[0] == 1 else ">"
    geometry_type = struct.unpack_from(endian + "I", payload, 1)[0]
    # The public EWKB reader restores SRID on Shapely 1.x as well as 2.x.
    # Convert the root ISO type offset to EWKB flags before adding its SRID.
    plain = geometry_type & 0x1FFFFFFF
    layout, base = divmod(plain, 1000)
    if layout > 3 or not 1 <= base <= 7:
        raise ValueError("Invalid geometry WKB type")
    flags = geometry_type & 0xC0000000
    if layout in (1, 3):
        flags |= 0x80000000
    if layout in (2, 3):
        flags |= 0x40000000
    offset = 9 if geometry_type & 0x20000000 else 5
    ewkb = (
        payload[:1]
        + struct.pack(endian + "Ii", base | flags | 0x20000000, srid)
        + payload[offset:]
    )
    return geometry_wkb.loads(ewkb)


def _iso_headers(source):
    """Adapt the legacy public writer's little-endian EWKB headers to ISO WKB."""
    result = bytearray()
    position = 0
    pending = 1

    def copy(size):
        nonlocal position
        end = position + size
        if end > len(source):
            raise ValueError("Invalid geometry WKB framing")
        result.extend(source[position:end])
        position = end

    def count():
        value = struct.unpack_from("<I", source, position)[0]
        copy(4)
        return value

    while pending:
        pending -= 1
        geometry_type = struct.unpack_from("<I", source, position + 1)[0]
        base = geometry_type & 0x1FFFFFFF
        has_z = bool(geometry_type & 0x80000000)
        has_m = bool(geometry_type & 0x40000000)
        dimensions = 2 + has_z + has_m
        position += 9 if geometry_type & 0x20000000 else 5
        result.extend(struct.pack("<BI", 1, base + 1000 * has_z + 2000 * has_m))
        if base == 1:
            copy(dimensions * 8)
        elif base == 2:
            copy(count() * dimensions * 8)
        elif base == 3:
            for _ in range(count()):
                copy(count() * dimensions * 8)
        elif 4 <= base <= 7:
            pending += count()
        else:
            raise ValueError("Invalid geometry WKB type")
    return bytes(result)
