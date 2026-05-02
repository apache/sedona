# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pyspark.sql.types import (
    BinaryType,
    DoubleType,
    StructField,
    StructType,
    UserDefinedType,
)

# Only support RasterType when rasterio is installed
try:
    import rasterio
except ImportError:
    rasterio = None

if rasterio is not None:
    from sedona.spark.raster import raster_serde
    from sedona.spark.raster.sedona_raster import SedonaRaster
else:
    # We'll skip RasterType UDT registration and raise error when deserializing
    # RasterUDT objects if rasterio is not installed
    raster_serde = None
    SedonaRaster = None

from sedona.spark.utils import geometry_serde
from sedona.spark.core.geom.geography import Geography
from sedona.spark.core.geom.box2d import Box2D


class GeometryType(UserDefinedType):

    @classmethod
    def sqlType(cls):
        return BinaryType()

    def serialize(self, obj):
        return geometry_serde.serialize(obj)

    def deserialize(self, datum):
        geom, offset = geometry_serde.deserialize(datum)
        return geom

    @classmethod
    def module(cls):
        return "sedona.spark.sql.types"

    def needConversion(self):
        return True

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.sql.sedona_sql.UDT.GeometryUDT"


class GeographyType(UserDefinedType):

    @classmethod
    def sqlType(cls):
        return BinaryType()

    def serialize(self, obj):
        return geometry_serde.serialize(obj.geometry)

    def deserialize(self, datum):
        geom, offset = geometry_serde.deserialize(datum)
        return Geography(geom)

    @classmethod
    def module(cls):
        return "sedona.spark.sql.types"

    def needConversion(self):
        return True

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.sql.sedona_sql.UDT.GeographyUDT"


class Box2DType(UserDefinedType):

    @classmethod
    def sqlType(cls):
        return StructType(
            [
                StructField("xmin", DoubleType(), nullable=False),
                StructField("ymin", DoubleType(), nullable=False),
                StructField("xmax", DoubleType(), nullable=False),
                StructField("ymax", DoubleType(), nullable=False),
            ]
        )

    def serialize(self, obj):
        return (obj.xmin, obj.ymin, obj.xmax, obj.ymax)

    def deserialize(self, datum):
        return Box2D(datum[0], datum[1], datum[2], datum[3])

    @classmethod
    def module(cls):
        return "sedona.spark.sql.types"

    def needConversion(self):
        return True

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.sql.sedona_sql.UDT.Box2DUDT"


class RasterType(UserDefinedType):

    @classmethod
    def sqlType(cls):
        return BinaryType()

    def serialize(self, obj):
        raise NotImplementedError("RasterType.serialize is not implemented yet")

    def deserialize(self, datum):
        if raster_serde is not None:
            return raster_serde.deserialize(datum)
        else:
            raise NotImplementedError(
                "rasterio is not installed. Please install it to support RasterType deserialization"
            )

    @classmethod
    def module(cls):
        return "sedona.spark.sql.types"

    def needConversion(self):
        return True

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.sql.sedona_sql.UDT.RasterUDT"
