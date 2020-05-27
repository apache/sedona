from abc import ABC

import attr
from shapely.geometry.base import BaseGeometry


@attr.s
class GeometryParser(ABC):

    @property
    def name(self):
        raise NotImplementedError

    @classmethod
    def serialize(cls, obj: BaseGeometry, binary_buffer: 'BinaryBuffer'):
        raise NotImplementedError("Parser has to implement serialize method")

    @classmethod
    def deserialize(cls, bin_parser: 'BinaryParser') -> BaseGeometry:
        raise NotImplementedError("Parser has to implement deserialize method")

