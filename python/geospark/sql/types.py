import struct

from pyspark.sql.types import UserDefinedType, ArrayType, ByteType
from shapely.wkb import dumps, loads


class GeometryType(UserDefinedType):

    @classmethod
    def sqlType(cls):
        return ArrayType(ByteType(), containsNull=False)

    def fromInternal(self, obj):
        return self.deserialize(obj)

    def toInternal(self, obj):
        return [el - 256 if el >= 128 else el for el in self.serialize(obj)]

    def serialize(self, obj):
        return dumps(obj)

    def deserialize(self, datum):
        bytes_data = b''.join([struct.pack('b', el) for el in datum])
        geom = loads(bytes_data)
        return geom

    @classmethod
    def module(cls):
        return "geospark.sql.types"

    def needConversion(self):
        return True

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.sql.geosparksql.UDT.GeometryUDT"
