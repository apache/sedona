from typing import List

import attr
from shapely.geometry.base import BaseGeometry

from geo_pyspark.sql.enums import GeomEnum
from geo_pyspark.sql.exceptions import GeometryUnavailableException
from geo_pyspark.utils.abstract_parser import GeometryParser
from geo_pyspark.utils.binary_parser import BinaryParser, BinaryBuffer
from geo_pyspark.utils.decorators import classproperty
from geo_pyspark.utils.parsers import MultiPointParser, PolygonParser, PolyLineParser, PointParser, UndefinedParser, \
    MultiLineStringParser, LineStringParser, MultiPolygonParser


@attr.s
class GeometryFactory:

    @classmethod
    def geometry_from_bytes(cls, bytes: bytearray) -> BaseGeometry:
        bin_parser = BinaryParser(bytes)
        g_type = bin_parser.read_byte()
        gm_type = bin_parser.read_byte()
        if GeomEnum.has_value(gm_type):
            name = GeomEnum.get_name(gm_type)
            parser: GeometryParser = cls.parsers[name]
            geom = parser.deserialize(bin_parser)
            return geom
        else:
            raise GeometryUnavailableException(f"Can not deserialize object")

    @classmethod
    def to_bytes(cls, geom: BaseGeometry) -> List[int]:
        geom_name = str(geom.__class__.__name__).lower()

        try:
            appr_parser = cls.parsers[geom_name]
        except KeyError:
            raise KeyError(f"Parser for geometry {geom_name}")
        return appr_parser.serialize(geom, BinaryBuffer())

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
