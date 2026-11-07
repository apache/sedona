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

"""Public Shapely WKB/EWKB helpers with compatibility for supported legacy GEOS."""

import math
import struct

import shapely
from shapely import wkb, wkt
from shapely.geometry import Point

try:
    from shapely import geos_version
except ImportError:
    from shapely.geos import geos_version


def dumps(geometry):
    """Write EWKB, retaining SRID, declared empty type and supported dimensions."""
    if geometry is None:
        return None
    if geos_version < (3, 9, 0):
        if isinstance(geometry, Point) and geometry.is_empty:
            # Older GEOS cannot emit empty Point WKB. Only its standard header
            # and NaN coordinates need a compatibility encoding.
            dimensions = _empty_point_dimension(geometry)
            return _legacy_header(geometry, 1, dimensions > 2) + struct.pack(
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
            payloads = [dumps(member) for member in members]
            has_z = any(
                struct.unpack_from("<I", value, 1)[0] & 0x80000000 for value in payloads
            )
            return (
                _legacy_header(geometry, collection_type, has_z)
                + struct.pack("<I", len(members))
                + b"".join(payloads)
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


def loads(value):
    """Decode WKB/EWKB without retaining a mutable input buffer."""
    return None if value is None else wkb.loads(bytes(value))


def _legacy_header(geometry, geometry_type, has_z):
    srid = _get_srid(geometry)
    flags = (0x80000000 if has_z else 0) | (0x20000000 if srid else 0)
    header = struct.pack("<BI", 1, geometry_type | flags)
    return header + struct.pack("<i", srid) if srid else header


def _empty_point_dimension(geometry):
    if hasattr(shapely, "get_coordinate_dimension"):
        return int(shapely.get_coordinate_dimension(geometry))
    if geometry.geom_type == "GeometryCollection":
        # Shapely 1.x Point() can hold an empty GeometryCollection.
        return 2
    # has_z and _ndim can both lose Z on empty points. Use Shapely 1.x's
    # existing coordinate-sequence bindings, without introducing a native API.
    from ctypes import byref, c_uint
    from shapely.geos import lgeos

    sequence = lgeos.GEOSGeom_getCoordSeq(geometry._geom)
    if not sequence:
        return 2
    dimensions = c_uint()
    if not lgeos.GEOSCoordSeq_getDimensions(sequence, byref(dimensions)):
        raise ValueError("Cannot read empty Point coordinate dimension")
    return dimensions.value


def _get_srid(geometry):
    if hasattr(shapely, "get_srid"):
        return shapely.get_srid(geometry)
    # Shapely 1.x has no public SRID getter, and its WKB writer cannot be
    # used to recover the SRID of an empty Point on GEOS < 3.9.
    from shapely.geos import lgeos

    return lgeos.GEOSGetSRID(geometry._geom)
