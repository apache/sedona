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

