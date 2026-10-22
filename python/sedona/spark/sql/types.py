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
from sedona.spark.core.geom.box3d import Box3D


class GeometryType(UserDefinedType):
    """Spark SQL type of Sedona geometry columns.

    Geometry values are Shapely geometries in Python: ``DataFrame.collect()``
    returns Shapely geometry objects, and Shapely geometries passed to
    ``createDataFrame`` are stored as geometries. Use ``GeometryType()`` in a
    schema to declare a geometry column.

    Examples
    --------
    >>> from pyspark.sql.types import StructField, StructType
    >>> from shapely.geometry import Point
    >>> from sedona.spark.sql.types import GeometryType
    >>> schema = StructType([StructField("geom", GeometryType())])
    >>> df = sedona.createDataFrame([(Point(1, 2),)], schema)  # doctest: +SKIP
    """

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
    """Spark SQL type of Sedona geography columns.

    Geography values are :class:`sedona.spark.core.geom.geography.Geography`
    objects in Python, which hold a Shapely geometry in their ``geometry``
    attribute. ``DataFrame.collect()`` returns them, and ``Geography`` objects
    passed to ``createDataFrame`` are stored as geographies.
    """

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
    """Spark SQL type of Sedona ``box2d`` columns, such as the result of ``ST_Box2D``.

    Values are :class:`sedona.spark.core.geom.box2d.Box2D` objects in Python, with
    ``xmin``, ``ymin``, ``xmax`` and ``ymax`` attributes. A column of this type is
    stored as a struct of those four doubles.
    """

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
    """Spark SQL type of Sedona ``box3d`` columns, such as the result of ``ST_Box3D``.

    Values are :class:`sedona.spark.core.geom.box3d.Box3D` objects in Python, with
    ``xmin``, ``ymin``, ``zmin``, ``xmax``, ``ymax`` and ``zmax`` attributes. A
    column of this type is stored as a struct of those six doubles.
    """

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
    """Spark SQL type of Sedona raster columns.

    Raster values are :class:`sedona.spark.raster.SedonaRaster` objects in
    Python, which ``DataFrame.collect()`` and Python UDFs receive. Converting
    raster values requires ``rasterio``; without it, they raise
    ``NotImplementedError``. Only in-db rasters can be converted back to a
    raster column, for example when a UDF returns one.
    """

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
