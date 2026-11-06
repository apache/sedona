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

"""Public EWKB transport for SQL geometry values, separate from RDD serialization."""

import math
import struct

import shapely
from shapely import wkb, wkt
from shapely.geometry import Point

try:
    from shapely import geos_version
except ImportError:
    from shapely.geos import geos_version


def serialize_geometry(geometry):
    """Encode a SQL geometry, retaining its SRID and supported coordinate dimensions."""
    if geometry is None:
        return None
    if geos_version < (3, 9, 0):
        if isinstance(geometry, Point) and geometry.is_empty:
            # Older GEOS cannot emit empty Point WKB. Only its standard header
            # and NaN coordinates need a compatibility encoding.
            dimensions = 3 if geometry.has_z else 2
            return _legacy_header(geometry, 1) + struct.pack(
                "<" + "d" * dimensions, *([math.nan] * dimensions)
            )
        collection_type = {"MultiPoint": 4, "GeometryCollection": 7}.get(
            type(geometry).__name__
        )
        if collection_type is not None:
            if hasattr(shapely, "get_parts"):
                members = list(geometry.geoms)
            else:
                # Shapely 1.x .geoms hides all-empty collections. Its sequence
                # wrapper retains their members without new native bindings.
                from shapely.geometry.base import HeterogeneousGeometrySequence

                members = list(HeterogeneousGeometrySequence(geometry))
            return (
                _legacy_header(geometry, collection_type)
                + struct.pack("<I", len(members))
                + b"".join(serialize_geometry(member) for member in members)
            )
    if hasattr(shapely, "to_wkb"):
        return shapely.to_wkb(
            geometry,
            byte_order=1,
            include_srid=True,
            output_dimension=(
                4
                if hasattr(shapely, "has_m") and shapely.geos_version >= (3, 12, 0)
                else 3
            ),
        )

    # Shapely 1.x empty constructors can hold a GEOS GeometryCollection even
    # when their Python class is Point, LineString, Polygon, or a multipart type.
    srid = None
    if geometry.is_empty and geometry.geom_type == "GeometryCollection":
        geometry_type = type(geometry).__name__
        if geometry_type != "GeometryCollection":
            if geometry_type == "LinearRing":
                geometry_type = "LineString"
            srid = _get_srid(geometry)
            geometry = wkt.loads(geometry_type.upper() + " EMPTY")
    return wkb.dumps(
        geometry, output_dimension=3, big_endian=False, include_srid=True, srid=srid
    )


def deserialize_geometry(value):
    """Decode WKB/EWKB without retaining a mutable input buffer."""
    return None if value is None else wkb.loads(bytes(value))


def _legacy_header(geometry, geometry_type):
    srid = _get_srid(geometry)
    flags = (0x80000000 if geometry.has_z else 0) | (0x20000000 if srid else 0)
    header = struct.pack("<BI", 1, geometry_type | flags)
    return header + struct.pack("<i", srid) if srid else header


def _get_srid(geometry):
    if hasattr(shapely, "get_srid"):
        return shapely.get_srid(geometry)
    # Shapely 1.x has no public SRID getter, and its WKB writer cannot be
    # used to recover the SRID of an empty Point on GEOS < 3.9.
    from shapely.geos import lgeos

    return lgeos.GEOSGetSRID(geometry._geom)
