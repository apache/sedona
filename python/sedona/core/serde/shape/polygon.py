#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from typing import Union

import attr
from shapely.geometry import Polygon, MultiPolygon, LinearRing

from sedona.core.serde.binary.buffer import BinaryBuffer
from sedona.core.serde.binary.order import ByteOrderType
from sedona.core.serde.binary.parser import BinaryParser
from sedona.core.serde.shape.enums import GeomEnum
from sedona.core.serde.shape.offset import OffsetsReader
from sedona.core.serde.shape.util import add_shape_geometry_metadata, get_number_of_rings, get_number_of_polygon_points, \
    add_offsets_to_polygon, reverse_linear_ring, put_coordinates, read_coordinates
from sedona.utils.abstract_parser import GeometryParser


@attr.s
class PolygonParser(GeometryParser):
    name = "Polygon"

    @classmethod
    def serialize(cls, obj: Polygon, binary_buffer: BinaryBuffer):
        if isinstance(obj, Polygon):
            add_shape_geometry_metadata(GeomEnum.polygon.value, binary_buffer)

            num_rings = get_number_of_rings(obj)

            num_points = get_number_of_polygon_points(obj)

            binary_buffer.add_empty_bytes("double", 4)

            binary_buffer.put_int(num_rings, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_int(num_points, ByteOrderType.LITTLE_ENDIAN)

            add_offsets_to_polygon(obj, binary_buffer, 0)

            coordinates_exterior = reverse_linear_ring(obj.exterior, False)
            put_coordinates(coordinates_exterior, binary_buffer)

            for ring in obj.interiors:
                coordinates = reverse_linear_ring(ring)
                put_coordinates(coordinates, binary_buffer)

            binary_buffer.put_byte(-127)

        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> Union[Polygon, MultiPolygon]:
        for _ in range(4):
            parser.read_double(ByteOrderType.LITTLE_ENDIAN)
        num_rings = parser.read_int(ByteOrderType.LITTLE_ENDIAN)
        num_points = parser.read_int(ByteOrderType.LITTLE_ENDIAN)
        offsets = OffsetsReader.read_offsets(parser, num_parts=num_rings, max_offset=num_points)
        polygons = []
        holes = []
        shells_ccw = False
        shell = None
        for i in range(num_rings):
            read_scale = offsets[i + 1] - offsets[i]
            cs_ring = read_coordinates(parser, read_scale)
            if (len(cs_ring)) < 3:
                continue

            ring = LinearRing(cs_ring)

            if shell is None:
                shell = ring
                shells_ccw = LinearRing(cs_ring).is_ccw
            elif LinearRing(cs_ring).is_ccw != shells_ccw:
                holes.append(ring)
            else:
                if shell is not None:
                    polygon = Polygon(shell, holes)
                    polygons.append(polygon)
                shell = ring
                holes = []

        if shell is not None:
            geometry = Polygon(shell, holes)
            polygons.append(geometry)

        if polygons.__len__() == 1:
            return polygons[0]

        return MultiPolygon(polygons)
