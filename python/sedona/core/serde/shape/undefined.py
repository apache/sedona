import attr
from shapely.geometry.base import BaseGeometry

from sedona.core.serde.binary.buffer import BinaryBuffer
from sedona.core.serde.binary.parser import BinaryParser
from sedona.utils.abstract_parser import GeometryParser


@attr.s
class UndefinedParser(GeometryParser):
    name = "Undefined"

    @classmethod
    def serialize(cls, obj: BaseGeometry, binary_buffer: BinaryBuffer):
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> BaseGeometry:
        raise NotImplementedError()
