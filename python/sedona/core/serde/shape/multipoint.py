import attr
from shapely.geometry import MultiPoint

from sedona.core.serde.binary.buffer import BinaryBuffer
from sedona.core.serde.binary.order import ByteOrderType
from sedona.core.serde.binary.parser import BinaryParser
from sedona.core.serde.shape.enums import GeomEnum
from sedona.core.serde.shape.util import add_shape_geometry_metadata, read_coordinates
from sedona.utils.abstract_parser import GeometryParser


@attr.s
class MultiPointParser(GeometryParser):
    name = "MultiPoint"

    @classmethod
    def serialize(cls, obj: MultiPoint, binary_buffer: BinaryBuffer):
        if isinstance(obj, MultiPoint):
            binary_buffer.put_int(0, ByteOrderType.LITTLE_ENDIAN)
            add_shape_geometry_metadata(GeomEnum.multipoint.value, binary_buffer)
            binary_buffer.add_empty_bytes("double", 4, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_int(len(obj.geoms), ByteOrderType.LITTLE_ENDIAN)
            for point in obj.geoms:
                binary_buffer.put_double(point.x, ByteOrderType.LITTLE_ENDIAN)
                binary_buffer.put_double(point.y, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_byte(-127)
        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> MultiPoint:
        for _ in range(4):
            parser.read_double(ByteOrderType.LITTLE_ENDIAN)
        number_of_points = parser.read_int(ByteOrderType.LITTLE_ENDIAN)

        coordinates = read_coordinates(parser, number_of_points)

        return MultiPoint(coordinates)