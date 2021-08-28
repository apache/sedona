from typing import List

from shapely.geometry.base import BaseGeometry

from sedona.core.serde.binary.buffer import BinaryBuffer
from sedona.core.serde.binary.parser import BinaryParser


class GeometrySerde:

    byte_number = None

    def deserialize(self, bin_parser: BinaryParser) -> BaseGeometry:
        raise NotImplementedError("child class should implement method geometry from bytes")

    def serialize(self, geom: BaseGeometry, buffer: BinaryBuffer) -> List[int]:
        raise NotImplementedError("child class should implement method to bytes")
