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
from shapely.geometry import MultiLineString, LineString

from sedona.core.serde.binary.buffer import BinaryBuffer
from sedona.core.serde.binary.order import ByteOrderType
from sedona.core.serde.binary.parser import BinaryParser
from sedona.core.serde.shape.offset import OffsetsReader
from sedona.core.serde.shape.util import read_coordinates
from sedona.sql.exceptions import InvalidGeometryException
from sedona.utils.abstract_parser import GeometryParser


@attr.s
class PolyLineParser(GeometryParser):
    name = "Polyline"

    @classmethod
    def serialize(cls, obj: Union[MultiLineString, LineString], binary_buffer: BinaryBuffer):
        raise NotImplementedError("")

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> Union[LineString, MultiLineString]:
        for _ in range(4):
            parser.read_double(ByteOrderType.LITTLE_ENDIAN)

        num_parts = parser.read_int(ByteOrderType.LITTLE_ENDIAN)
        num_points = parser.read_int(ByteOrderType.LITTLE_ENDIAN)

        offsets = OffsetsReader.read_offsets(parser, num_parts, num_points)
        lines = []
        for i in range(num_parts):
            read_scale = offsets[i + 1] - offsets[i]
            coordinate_sequence = read_coordinates(parser, read_scale)
            lines.append(LineString(coordinate_sequence))

        if num_parts == 1:
            line = lines[0]
        elif num_parts > 1:
            line = MultiLineString(lines)
        else:
            raise InvalidGeometryException("Invalid geometry")

        return line
