from typing import List

import attr
from shapely.geometry.base import BaseGeometry

from geo_pyspark.sql.enums import GeomEnum, ShapeEnum
from geo_pyspark.sql.exceptions import GeometryUnavailableException
from geo_pyspark.utils.abstract_parser import GeometryParser
from geo_pyspark.utils.binary_parser import BinaryParser, BinaryBuffer
from geo_pyspark.utils.parsers import CircleParser, PARSERS


@attr.s
class GeometryFactory:

    @classmethod
    def geometry_from_bytes(cls, bin_parser: BinaryParser) -> BaseGeometry:
        g_type = bin_parser.read_byte()
        shape_type = ShapeEnum.get_name(g_type)

        if shape_type == ShapeEnum.circle.name:
            return CircleParser.deserialize(bin_parser)

        elif shape_type == ShapeEnum.shape.name:
            gm_type = bin_parser.read_byte()
            if GeomEnum.has_value(gm_type):
                name = GeomEnum.get_name(gm_type)
                parser: GeometryParser = PARSERS[name]
                geom = parser.deserialize(bin_parser)
                return geom
            else:
                raise GeometryUnavailableException(f"Can not deserialize object")

    @classmethod
    def to_bytes(cls, geom: BaseGeometry) -> List[int]:
        from geo_pyspark.sql.types import GeometryType
        geom_name = str(geom.__class__.__name__).lower()

        try:
            appr_parser = PARSERS[geom_name]
            geom.__UDT__ = GeometryType()
        except KeyError:
            raise KeyError(f"Parser for geometry {geom_name} is not available")
        return appr_parser.serialize(geom, BinaryBuffer())
