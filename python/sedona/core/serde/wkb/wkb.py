from typing import List

from shapely.geometry.base import BaseGeometry
from shapely.wkb import dumps

from sedona.core.serde.binary.buffer import BinaryBuffer
from sedona.core.serde.binary.order import ByteOrderType
from sedona.core.serde.binary.parser import BinaryParser
from sedona.core.serde.serializer import GeometrySerde


class WkbSerde(GeometrySerde):

    byte_number = 1

    def deserialize(self, bin_parser: BinaryParser) -> BaseGeometry:
        geom_length = bin_parser.read_int(ByteOrderType.BIG_ENDIAN)
        geom = bin_parser.read_geometry(geom_length)
        return geom

    def serialize(self, geom: BaseGeometry, buffer: BinaryBuffer) -> List[int]:
        geom_bytes = dumps(geom, srid=4326)
        buffer.put_byte(self.byte_number)
        buffer.put_int(len(geom_bytes), ByteOrderType.BIG_ENDIAN)
        return [*buffer.byte_array, *geom_bytes, *[0]]
