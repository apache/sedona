from pyspark.sql.types import UserDefinedType, ArrayType, ByteType

from geo_pyspark.sql.geometry import GeometryFactory


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
        geom = GeometryFactory.geometry_from_bytes(datum)

        return geom

    @classmethod
    def module(cls):
        pass

    def needConversion(self):
        return True

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.sql.geosparksql.UDT.GeometryUDT"
