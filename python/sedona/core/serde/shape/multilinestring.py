from typing import Union

import attr
from shapely.geometry import MultiLineString, LineString

from sedona.core.serde.binary.buffer import BinaryBuffer
from sedona.core.serde.binary.parser import BinaryParser
from sedona.core.serde.shape.enums import GeomEnum
from sedona.core.serde.shape.util import add_shape_geometry_metadata, put_coordinates
from sedona.utils.abstract_parser import GeometryParser
from sedona.utils.binary_parser import ByteOrderType


@attr.s
class MultiLineStringParser(GeometryParser):
    name = "MultiLineString"

    @classmethod
    def serialize(cls, obj: MultiLineString, binary_buffer: BinaryBuffer):
        if isinstance(obj, MultiLineString):
            add_shape_geometry_metadata(GeomEnum.polyline.value, binary_buffer)
            binary_buffer.add_empty_bytes("double", 4)

            num_parts = len(obj.geoms)
            num_points = sum([len(el.coords) for el in obj.geoms])

            binary_buffer.put_int(num_parts, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_int(num_points, ByteOrderType.LITTLE_ENDIAN)

            offset = 0
            for _ in range(num_parts):
                binary_buffer.put_int(offset, ByteOrderType.LITTLE_ENDIAN)
                offset = offset + obj.geoms[_].coords.__len__()

            for geom in obj.geoms:
                put_coordinates(geom.coords, binary_buffer)

            binary_buffer.put_byte(-127)
        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> Union[LineString, MultiLineString]:
        raise NotImplemented()