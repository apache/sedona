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

import pyspark

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
from sedona.spark.core.geom.box3d import Box3D


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


class LegacyGeometryType(GeometryType):
    """Sedona's binary geometry UDT, including when Spark uses native spatial types."""


class LegacyGeographyType(GeographyType):
    """Persisted geography UDT: big-endian SRID followed by WKB, as on the JVM."""

    def serialize(self, obj):
        import struct
        import shapely

        geometry = obj.geometry
        return struct.pack(">i", int(shapely.get_srid(geometry))) + shapely.to_wkb(
            geometry, flavor="iso", include_srid=False
        )

    def deserialize(self, datum):
        import struct
        import shapely

        srid = struct.unpack(">i", datum[:4])[0]
        return Geography(shapely.set_srid(shapely.from_wkb(bytes(datum[4:])), srid))


# Spark 4.1 exposes preliminary spatial types, but native Sedona integration
# starts with Spark 4.2. Keep the existing UDTs on every older runtime.
USES_NATIVE_SPATIAL_TYPES = tuple(
    int(part) for part in pyspark.__version__.split(".")[:2]
) >= (4, 2)

if USES_NATIVE_SPATIAL_TYPES:
    from pyspark.sql.types import GeographyType, GeometryType


def geometry_type():
    """Default spatial schema; native schemas allow a different SRID in each row."""
    return GeometryType("ANY") if USES_NATIVE_SPATIAL_TYPES else GeometryType()


def geography_type():
    """Default geography schema for the installed Spark runtime."""
    return GeographyType("ANY") if USES_NATIVE_SPATIAL_TYPES else GeographyType()


def to_spark_geometry(geometry):
    """Convert a Shapely geometry to Spark's spatial value, preserving its SRID.

    Spark 4.2+ returns a native ``pyspark.sql.types.Geometry``. Older Spark
    versions accept Shapely directly through Sedona's UDT and return it unchanged.
    ``None`` is preserved. Native conversion requires Shapely 2 or later.
    """
    if geometry is None or not USES_NATIVE_SPATIAL_TYPES:
        return geometry
    import shapely
    from pyspark.sql.types import Geometry

    if isinstance(geometry, Geometry):
        return geometry
    return Geometry(
        shapely.to_wkb(geometry, flavor="iso", include_srid=False),
        int(shapely.get_srid(geometry)),
    )


def to_spark_geography(geometry):
    """Convert a Shapely geometry (or Sedona Geography) to a Spark geography.

    On Spark 4.2+, an unset Shapely SRID (0) defaults to WGS84 (4326), matching
    Spark's native geography constructor. Other embedded SRIDs are preserved.
    Older Spark returns Sedona's Geography wrapper. ``None`` is preserved.
    """
    if geometry is None:
        return None
    if isinstance(geometry, Geography):
        if not USES_NATIVE_SPATIAL_TYPES:
            return geometry
        geometry = geometry.geometry
    if not USES_NATIVE_SPATIAL_TYPES:
        return Geography(geometry)
    import shapely
    from pyspark.sql.types import Geography as NativeGeography

    if isinstance(geometry, NativeGeography):
        return geometry
    srid = int(shapely.get_srid(geometry)) or 4326
    # Validate geographic SRIDs using Spark's own supported CRS mapping.
    GeographyType(srid)
    return NativeGeography(
        shapely.to_wkb(geometry, flavor="iso", include_srid=False), srid
    )


def to_shapely(value):
    """Convert a collected native geometry/geography to Shapely, preserving SRID.

    Shapely values from legacy Sedona UDTs and ``None`` pass through unchanged.
    Sedona Geography wrappers are unwrapped. Native conversion requires Shapely 2+.
    """
    if value is None:
        return None
    if isinstance(value, Geography):
        return value.geometry
    if USES_NATIVE_SPATIAL_TYPES:
        from pyspark.sql.types import Geography as NativeGeography, Geometry

        if isinstance(value, (Geometry, NativeGeography)):
            import shapely

            return shapely.set_srid(shapely.from_wkb(bytes(value.wkb)), value.srid)
    return value


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


class Box3DType(UserDefinedType):

    @classmethod
    def sqlType(cls):
        return StructType(
            [
                StructField("xmin", DoubleType(), nullable=False),
                StructField("ymin", DoubleType(), nullable=False),
                StructField("zmin", DoubleType(), nullable=False),
                StructField("xmax", DoubleType(), nullable=False),
                StructField("ymax", DoubleType(), nullable=False),
                StructField("zmax", DoubleType(), nullable=False),
            ]
        )

    def serialize(self, obj):
        return (obj.xmin, obj.ymin, obj.zmin, obj.xmax, obj.ymax, obj.zmax)

    def deserialize(self, datum):
        return Box3D(datum[0], datum[1], datum[2], datum[3], datum[4], datum[5])

    @classmethod
    def module(cls):
        return "sedona.spark.sql.types"

    def needConversion(self):
        return True

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.sql.sedona_sql.UDT.Box3DUDT"


class RasterType(UserDefinedType):

    @classmethod
    def sqlType(cls):
        return BinaryType()

    def serialize(self, obj):
        if raster_serde is None:
            raise NotImplementedError(
                "rasterio is not installed. Please install it to support "
                "RasterType serialization"
            )
        return raster_serde.serialize(obj)

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
