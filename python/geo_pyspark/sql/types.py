from pyspark.sql.types import UserDefinedType, ArrayType, ByteType


class GeometryType(UserDefinedType):

    @classmethod
    def sqlType(cls):
        return ArrayType(ByteType(), containsNull=False)

    def fromInternal(self, obj):
        return self.deserialize(obj)

    def toInternal(self, obj):
        return self.serialize(obj)

    def serialize(self, obj):
        from geo_pyspark.sql.geometry import GeometryFactory
        return GeometryFactory.to_bytes(obj)

    def deserialize(self, datum):
        from geo_pyspark.sql.geometry import GeometryFactory
        from geo_pyspark.utils.binary_parser import BinaryParser

        bin_parser = BinaryParser(datum)
        geom = GeometryFactory.geometry_from_bytes(bin_parser)

        return geom

    @classmethod
    def module(cls):
        return "geo_pyspark.sql.types"

    def needConversion(self):
        return True

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.sql.geosparksql.UDT.GeometryUDT"
