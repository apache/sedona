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
from enum import Enum

from pyspark.sql.types import UserDefinedType, ArrayType, ByteType

from sedona.sql.geometry import GeometryFactory, ShapeSerde, WkbSerde
from sedona.utils.binary_parser import BinaryParser, ByteOrderType, BinaryBuffer
from pyspark.sql import SparkSession

geometry_serializers = {
    0: ShapeSerde(),
    1: WkbSerde()
}

serializers = {
    "shape": 0,
    "wkb": 1
}


class SparkConfGetter:

    def _get_serialization_type(self):
        current_spark = SparkSession.getActiveSession()
        if current_spark:
            spark_context = current_spark.sparkContext
            conf = spark_context.getConf()
            serializer_type = conf.get("sedona.serializer.type")
        else:
            serializer_type = "shape"

        return serializer_type

    @property
    def serialization(self):
        if not hasattr(self, "__serialization"):
            setattr(self, "__serialization", self._get_serialization_type())
        return getattr(self, "__serialization")


class GeometryType(UserDefinedType):

    @classmethod
    def sqlType(cls):
        return ArrayType(ByteType(), containsNull=False)

    def fromInternal(self, obj):
        return self.deserialize(obj)

    def toInternal(self, obj):
        return [el - 256 if el >= 128 else el for el in self.serialize(obj)]

    def serialize(self, obj):
        buffer = BinaryBuffer()
        serde = geometry_serializers[self.serializer_number]
        return serde.to_bytes(obj, buffer)

    def deserialize(self, datum):
        binary_parser = BinaryParser(datum)
        binary_parser.read_byte()
        parser_type = binary_parser.read_int(ByteOrderType.BIG_ENDIAN)
        print(",".join([str(el) for el in binary_parser.bytes]))
        return geometry_serializers[parser_type].geometry_from_bytes(binary_parser)

    @classmethod
    def module(cls):
        return "sedona.sql.types"

    def needConversion(self):
        return True

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.sql.sedona_sql.UDT.GeometryUDT"

    @property
    def serializer_number(self):
        if not hasattr(self, "__serializer_number"):
            setattr(self, "__serializer_number", serializers[SparkConfGetter().serialization])
        return getattr(self, "__serializer_number")
