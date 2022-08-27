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

from pyspark.sql.types import UserDefinedType, ArrayType, ByteType
from shapely.wkb import dumps, loads


class GeometryType(UserDefinedType):

    @classmethod
    def sqlType(cls):
        return ArrayType(ByteType(), containsNull=False)

    def fromInternal(self, obj):
        deserialized_obj = None
        if obj is not None:
            deserialized_obj = self.deserialize(obj)
        return deserialized_obj

    def toInternal(self, obj):
        serialized_obj = None
        if obj is not None:
            serialized_obj = [el - 256 if el >= 128 else el for el in self.serialize(obj)]
        return serialized_obj

    def serialize(self, obj):
        return dumps(obj)

    def deserialize(self, datum):
        bytes_data = b''.join([struct.pack('b', el) for el in datum])
        geom = loads(bytes_data)
        return geom

    @classmethod
    def module(cls):
        return "sedona.sql.types"

    def needConversion(self):
        return True

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.sql.sedona_sql.UDT.GeometryUDT"
