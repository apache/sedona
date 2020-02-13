from abc import ABC
from copy import copy
from typing import List, Any

import attr
from shapely.geometry.base import BaseGeometry
from pyspark import PickleSerializer

from geospark.sql.types import GeometryFactory
from geospark.utils.binary_parser import BinaryParser


class GeoData:

    def __init__(self, geom: BaseGeometry, userData: str):
        """

        :param geom:
        :param userData:
        """
        self._geom = geom
        self._userData = userData

    def getUserData(self):
        return self.userData

    def __getstate__(self):
        attributes = copy(self.__slots__)
        geom = getattr(self, attributes[0])
        return dict(
            geom=bytearray([el if el >= 0 else el + 256 for el in GeometryFactory.to_bytes(geom)]),
            userData=getattr(self, attributes[1])
        )

    def __setstate__(self, attributes):
        self._geom = GeometryFactory.geom_from_bytes_data(attributes["geom"])
        self._userData = attributes["userData"]

    @property
    def geom(self):
        return self._geom

    @property
    def userData(self):
        return self._userData

    __slots__ = ("_geom", "_userData")

    def __repr__(self):
        return f"Geometry: {str(self.geom.__class__.__name__)} userData: {self.userData}"

    def __eq__(self, other):
        return self.geom == other.geom and self.userData == other.userData

    def __ne__(self, other):
        return self.geom != other.geom or self.userData != other.userData


@attr.s
class AbstractSpatialRDDParser(ABC):

    @classmethod
    def serialize(cls, obj: List[Any], binary_buffer: 'BinaryBuffer') -> bytearray:
        raise NotImplemented()

    @classmethod
    def deserialize(cls, bin_parser: 'BinaryParser') -> BaseGeometry:
        raise NotImplementedError("Parser has to implement deserialize method")

    @classmethod
    def _deserialize_geom(cls, bin_parser: 'BinaryParser') -> GeoData:
        user_data_length = bin_parser.read_int()
        geom = GeometryFactory.geometry_from_bytes(bin_parser)
        if user_data_length > 0:
            user_data = bin_parser.read_string(user_data_length)
            geo_data = GeoData(geom=geom, userData=user_data)

        else:
            geo_data = GeoData(geom=geom, userData="")
        return geo_data


@attr.s
class SpatialPairRDDParserData(AbstractSpatialRDDParser):
    name = "SpatialPairRDDParserData"

    @classmethod
    def deserialize(cls, bin_parser: 'BinaryParser'):
        left_geom_data = cls._deserialize_geom(bin_parser)

        _ = bin_parser.read_int()

        right_geom_data = cls._deserialize_geom(bin_parser)

        deserialized_data = [left_geom_data, right_geom_data]

        return deserialized_data

    @classmethod
    def serialize(cls, obj: BaseGeometry, binary_buffer: 'BinaryBuffer'):
        raise NotImplementedError("Currently this operation is not supported")


@attr.s
class SpatialRDDParserData(AbstractSpatialRDDParser):
    name = "SpatialRDDParser"

    @classmethod
    def deserialize(cls, bin_parser: 'BinaryParser'):
        left_geom_data = cls._deserialize_geom(bin_parser)
        _ = bin_parser.read_int()

        return left_geom_data

    @classmethod
    def serialize(cls, obj: BaseGeometry, binary_buffer: 'BinaryBuffer'):
        raise NotImplementedError("Currently this operation is not supported")


@attr.s
class SpatialRDDParserDataMultipleRightGeom(AbstractSpatialRDDParser):
    name = "SpatialRDDParser"

    @classmethod
    def deserialize(cls, bin_parser: 'BinaryParser'):
        left_geom_data = cls._deserialize_geom(bin_parser)

        geometry_numbers = bin_parser.read_int()

        right_geoms = []

        for right_geometry_number in range(geometry_numbers):
            right_geom_data = cls._deserialize_geom(bin_parser)
            right_geoms.append(right_geom_data)

        deserialized_data = [left_geom_data, right_geoms] if right_geoms else left_geom_data

        return deserialized_data

    @classmethod
    def serialize(cls, obj: BaseGeometry, binary_buffer: 'BinaryBuffer'):
        raise NotImplementedError("Currently this operation is not supported")


PARSERS = {
    0: SpatialRDDParserData(),
    1: SpatialRDDParserDataMultipleRightGeom(),
    2: SpatialPairRDDParserData(),
}


class GeoSparkPickler(PickleSerializer):

    def __init__(self):
        super().__init__()

    def loads(self, obj, encoding="bytes"):
        binary_parser = BinaryParser(obj)
        spatial_parser_number = binary_parser.read_int()
        spatial_parser = self.get_parser(spatial_parser_number)
        parsed_row = spatial_parser.deserialize(binary_parser)

        return parsed_row

    def dumps(self, obj):
        raise NotImplementedError()

    def get_parser(self, number: int):
        return PARSERS[number]
