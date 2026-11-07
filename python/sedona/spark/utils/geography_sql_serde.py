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

"""Python transport for the existing JVM geography UDT: big-endian SRID and WKB."""

import struct

from sedona.spark.core.geom.geography import Geography
from sedona.spark.utils import geometry_wkb

_SRID_FLAG = 0x20000000


def serialize_geography(geography):
    """Encode a geography without changing the JVM geography or RDD formats."""
    if geography is None:
        return None
    payload = geometry_wkb.dumps(geography.geometry)
    geometry_type = struct.unpack_from("<I", payload, 1)[0]
    srid = 0
    if geometry_type & _SRID_FLAG:
        srid = struct.unpack_from("<i", payload, 5)[0]
        payload = struct.pack("<BI", 1, geometry_type & ~_SRID_FLAG) + payload[9:]
    if srid < 0:
        raise ValueError("Geography SRID must be non-negative")
    return struct.pack(">i", srid) + payload


def deserialize_geography(value):
    """Decode the JVM's SRID prefix using Shapely's public EWKB reader."""
    if value is None:
        return None
    value = bytes(value)
    if len(value) < 9:
        raise ValueError("Geography payload must contain a four-byte SRID and WKB")
    srid = struct.unpack_from(">i", value)[0]
    if srid < 0:
        raise ValueError("Geography SRID must be non-negative")
    payload = value[4:]
    if payload[0] not in (0, 1):
        raise ValueError("Invalid geography WKB byte order")
    endian = "<" if payload[0] == 1 else ">"
    geometry_type = struct.unpack_from(endian + "I", payload, 1)[0]
    offset = 9 if geometry_type & _SRID_FLAG else 5
    # The outer SRID is authoritative. Embedding it in EWKB lets both Shapely
    # 1.x and 2.x retain it without direct access to GEOS pointers.
    ewkb = (
        payload[:1]
        + struct.pack(endian + "Ii", geometry_type | _SRID_FLAG, srid)
        + payload[offset:]
    )
    return Geography(geometry_wkb.loads(ewkb))
