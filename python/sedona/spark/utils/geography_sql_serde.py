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

"""SQL geography transport: little-endian signed SRID followed by ISO WKB."""

import struct

from sedona.spark.core.geom.geography import Geography
from sedona.spark.utils import geometry_sql_serde


def serialize_geography(geography):
    """Encode SQL geography while preserving its non-negative SRID requirement."""
    if geography is None:
        return None
    payload = geometry_sql_serde.serialize_geometry(geography.geometry)
    if struct.unpack_from("<i", payload)[0] < 0:
        raise ValueError("Geography SRID must be non-negative")
    return payload


def deserialize_geography(value):
    """Decode SQL geography with the outer SRID as the authoritative value."""
    if value is None:
        return None
    value = bytes(value)
    if len(value) < 9:
        raise ValueError("Geography payload must contain a four-byte SRID and WKB")
    if struct.unpack_from("<i", value)[0] < 0:
        raise ValueError("Geography SRID must be non-negative")
    return Geography(geometry_sql_serde.deserialize_geometry(value))
