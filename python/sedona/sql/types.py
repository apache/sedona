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

from pyspark.sql.types import UserDefinedType, ArrayType, ByteType

from sedona.core.serde.geom_factory import GeometryFactory


class GeometryType(UserDefinedType):

    @classmethod
    def sqlType(cls):
        return ArrayType(ByteType(), containsNull=False)

    def fromInternal(self, obj):
        return self.deserialize(obj)

    def toInternal(self, obj):
        return [el - 256 if el >= 128 else el for el in self.serialize(obj)]

    def serialize(self, obj):
        return self.factory.serialize(obj)

    def deserialize(self, datum):
        return self.factory.deserialize(datum)

    @classmethod
    def module(cls):
        return "sedona.sql.types"

    def needConversion(self):
        return True

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.sql.sedona_sql.UDT.GeometryUDT"

    @property
    def factory(self):
        if not hasattr(self, "__factory"):
            setattr(self, "__factory", GeometryFactory.from_spark_config())
        return getattr(self, "__factory")
