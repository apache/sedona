import attr
from shapely.geometry import Point

from sedona.core.serde.binary.buffer import BinaryBuffer
from sedona.core.serde.binary.order import ByteOrderType
from sedona.core.serde.binary.parser import BinaryParser
from sedona.core.serde.shape.enums import GeomEnum
from sedona.core.serde.shape.util import add_shape_geometry_metadata
from sedona.utils.abstract_parser import GeometryParser


@attr.s
class PointParser(GeometryParser):
    name = "Point"

    @classmethod
    def serialize(cls, obj: Point, binary_buffer: BinaryBuffer):
        if isinstance(obj, Point):
            binary_buffer.put_int(0, ByteOrderType.LITTLE_ENDIAN)
            add_shape_geometry_metadata(GeomEnum.point.value, binary_buffer)
            binary_buffer.put_double(obj.x, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_double(obj.y, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_byte(0)
        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> Point:
        x = parser.read_double(ByteOrderType.LITTLE_ENDIAN)
        y = parser.read_double(ByteOrderType.LITTLE_ENDIAN)
        return Point(x, y)
