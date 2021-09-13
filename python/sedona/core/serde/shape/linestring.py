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
from shapely.geometry import LineString, MultiLineString

from sedona.core.serde.binary.buffer import BinaryBuffer
from sedona.core.serde.binary.order import ByteOrderType
from sedona.core.serde.binary.parser import BinaryParser
from sedona.core.serde.shape.enums import GeomEnum
from sedona.core.serde.shape.util import add_shape_geometry_metadata, put_coordinates
from sedona.utils.abstract_parser import GeometryParser


@attr.s
class LineStringParser(GeometryParser):
    name = "LineString"

    @classmethod
    def serialize(cls, obj: LineString, binary_buffer: BinaryBuffer):
        if isinstance(obj, LineString):
            add_shape_geometry_metadata(GeomEnum.polyline.value, binary_buffer)
            binary_buffer.add_empty_bytes("double", 4)
            num_points = obj.coords.__len__()
            binary_buffer.put_int(1, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_int(num_points, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_int(0, ByteOrderType.LITTLE_ENDIAN)

            put_coordinates(obj.coords, binary_buffer)

            binary_buffer.put_byte(-127)
        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> Union[LineString, MultiLineString]:
        raise NotImplemented()

