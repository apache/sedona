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

import struct
from abc import ABC
from copy import copy
from typing import List, Any

import attr
from shapely.geometry.base import BaseGeometry
try:
    from pyspark import CPickleSerializer
except ImportError:
    from pyspark import PickleSerializer as CPickleSerializer
from shapely.wkb import dumps

from sedona.core.geom.circle import Circle
from sedona.utils.binary_parser import BinaryParser


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

        if isinstance(geom, Circle):
            geom_bytes = CircleGeometryFactory.to_bytes(geom)
        else:
            geom_bytes = GeometryFactory.to_bytes(geom)

        return dict(
            geom=bytearray([el if el >= 0 else el + 256 for el in geom_bytes]),
            userData=getattr(self, attributes[1])
        )

    def __setstate__(self, attributes):
        bin_parser = BinaryParser(attributes["geom"])
        is_circle = bin_parser.read_byte()
        geom_bytes = attributes["geom"]

        if is_circle:
            radius = bin_parser.read_double()
            geom = bin_parser.read_geometry(geom_bytes.__len__() - 9)
            self._geom = Circle(geom, radius)
        else:
            self._geom = bin_parser.read_geometry(geom_bytes.__len__() - 1)

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
        is_circle = bin_parser.read_byte()
        return geom_deserializers[is_circle].geometry_from_bytes(bin_parser)


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


class SedonaPickler(CPickleSerializer):

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


def read_geometry_from_bytes(bin_parser: BinaryParser):
    geom_data_length = bin_parser.read_int()
    user_data_length = bin_parser.read_int()
    geom = bin_parser.read_geometry(geom_data_length)
    user_data = bin_parser.read_string(user_data_length)

    return (geom, user_data)


@attr.s
class GeometryFactory:

    @classmethod
    def geometry_from_bytes(cls, bin_parser: BinaryParser) -> GeoData:
        geom, user_data = read_geometry_from_bytes(bin_parser)
        geo_data = GeoData(geom=geom, userData=user_data)
        return geo_data

    @classmethod
    def to_bytes(cls, geom: BaseGeometry) -> List[int]:
        return struct.pack("b", 0) + dumps(geom)


@attr.s
class CircleGeometryFactory:

    @classmethod
    def geometry_from_bytes(cls, bin_parser: BinaryParser) -> GeoData:
        geom, user_data = read_geometry_from_bytes(bin_parser)
        radius = bin_parser.read_double()
        geo_data = GeoData(geom=Circle(geom, radius), userData=user_data)
        return geo_data

    @classmethod
    def to_bytes(cls, geom: Circle) -> List[int]:
        return struct.pack("b", 1) + struct.pack("d", geom.radius) + dumps(geom.centerGeometry)


geom_deserializers = {
    1: CircleGeometryFactory,
    0: GeometryFactory
}
