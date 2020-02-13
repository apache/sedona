from typing import List

import attr
from shapely.geometry.base import BaseGeometry
from pyspark.sql.types import UserDefinedType, ArrayType, ByteType

from geospark.sql.enums import GeomEnum, ShapeEnum
from geospark.sql.exceptions import GeometryUnavailableException
from geospark.utils.binary_parser import BinaryParser, BinaryBuffer
from geospark.utils.parsers import CircleParser, PARSERS


@attr.s
class GeometryFactory:

    @classmethod
    def geom_from_bytes_data(cls, bin_data: List):
        bin_parser = BinaryParser(bin_data)
        return cls.geometry_from_bytes(bin_parser)

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
                parser = PARSERS[name]
                geom = parser.deserialize(bin_parser)
                return geom
            else:
                raise GeometryUnavailableException(f"Can not deserialize object")

    @classmethod
    def to_bytes(cls, geom: BaseGeometry) -> List[int]:
        geom_name = str(geom.__class__.__name__).lower()

        try:
            appr_parser = PARSERS[geom_name]
            geom.__UDT__ = GeometryType()
        except KeyError:
            raise KeyError(f"Parser for geometry {geom_name} is not available")
        return appr_parser.serialize(geom, BinaryBuffer())


class GeometryType(UserDefinedType):

    @classmethod
    def sqlType(cls):
        return ArrayType(ByteType(), containsNull=False)

    def fromInternal(self, obj):
        return self.deserialize(obj)

    def toInternal(self, obj):
        return self.serialize(obj)

    def serialize(self, obj):
        return GeometryFactory.to_bytes(obj)

    def deserialize(self, datum):
        bin_parser = BinaryParser(datum)
        geom = GeometryFactory.geometry_from_bytes(bin_parser)

        return geom

    @classmethod
    def module(cls):
        return "geospark.sql.types"

    def needConversion(self):
        return True

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.sql.geosparksql.UDT.GeometryUDT"
