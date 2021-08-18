from typing import List

from shapely.geometry.base import BaseGeometry
from shapely.wkb import dumps

from sedona.sql.enums import GeomEnum
from sedona.sql.exceptions import GeometryUnavailableException
from sedona.utils.abstract_parser import GeometryParser
from sedona.utils.binary_parser import BinaryParser, BinaryBuffer, ByteOrderType
from sedona.utils.decorators import classproperty
from sedona.utils.parsers import MultiPointParser, PolygonParser, PolyLineParser, PointParser, UndefinedParser, \
    MultiLineStringParser, LineStringParser, MultiPolygonParser


class GeometrySerde:

    def geometry_from_bytes(self, bin_parser: BinaryParser) -> BaseGeometry:
        raise NotImplementedError("child class should implement method geometry from bytes")

    def to_bytes(self, geom: BaseGeometry, buffer: BinaryBuffer) -> List[int]:
        raise NotImplementedError("child class should implement method to bytes")


class WkbSerde(GeometrySerde):

    def geometry_from_bytes(self, bin_parser: BinaryParser) -> BaseGeometry:
        # bin_parser.read_byte()
        geom_length = bin_parser.read_int(ByteOrderType.BIG_ENDIAN)
        geom = bin_parser.read_geometry(geom_length)
        return geom

    def to_bytes(self, geom: BaseGeometry, buffer: BinaryBuffer) -> List[int]:
        buffer.put_byte(0)
        buffer.put_int(1, ByteOrderType.BIG_ENDIAN)
        geom_bytes = dumps(geom, srid=4326)
        buffer.put_int(len(geom_bytes), ByteOrderType.BIG_ENDIAN)
        return [*buffer.byte_array, *geom_bytes, *[0]]


class ShapeSerde(GeometrySerde):

    def geometry_from_bytes(self, bin_parser: BinaryParser) -> BaseGeometry:
        gm_type = bin_parser.read_byte()
        if GeomEnum.has_value(gm_type):
            name = GeomEnum.get_name(gm_type)
            parser: GeometryParser = self.parsers[name]
            geom = parser.deserialize(bin_parser)
            return geom
        else:
            raise GeometryUnavailableException(f"Can not deserialize object")

    def to_bytes(self, geom: BaseGeometry, buffer: BinaryBuffer) -> List[int]:
        geom_name = str(geom.__class__.__name__).lower()
        try:
            appr_parser = self.parsers[geom_name]
        except KeyError:
            raise KeyError(f"Parser for geometry {geom_name}")
        return appr_parser.serialize(geom, buffer)

    @classproperty
    def parsers(self):
        geom_cls = dict(
            undefined=UndefinedParser,
            point=PointParser,
            polyline=PolyLineParser,
            multilinestring=MultiLineStringParser,
            linestring=LineStringParser,
            polygon=PolygonParser,
            multipoint=MultiPointParser,
            multipolygon=MultiPolygonParser
        )
        return geom_cls


class GeometryFactory:

    def __init__(self, geometry_serde: 'GeometrySerde'):
        self.geometry_serde = geometry_serde

    def serialize(self, geom: BaseGeometry, binary_buffer: BinaryBuffer) -> List[int]:
        return self.geometry_serde.to_bytes(geom, binary_buffer)

    def deserialize(self, bytes: BinaryParser) -> BaseGeometry:
        return self.geometry_serde.geometry_from_bytes(bytes)
