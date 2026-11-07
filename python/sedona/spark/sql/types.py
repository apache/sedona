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

from sedona.spark.utils.geometry_sql_serde import (
    deserialize_geometry,
    serialize_geometry,
)
from sedona.spark.core.geom.geography import Geography
from sedona.spark.core.geom.box2d import Box2D
from sedona.spark.core.geom.box3d import Box3D
from sedona.spark.utils.geography_sql_serde import (
    deserialize_geography,
    serialize_geography,
)


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
        return serialize_geometry(obj)

    def deserialize(self, datum):
        return deserialize_geometry(datum)

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
        return serialize_geography(obj)

    def deserialize(self, datum):
        return deserialize_geography(datum)

    @classmethod
    def module(cls):
        return "sedona.spark.sql.types"

    def needConversion(self):
        return True

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.sql.sedona_sql.UDT.GeographyUDT"


# Spark 4.1 has preliminary spatial types, but Sedona's native integration
# starts in 4.2. Older Spark keeps its existing Shapely/Sedona UDT contract.
USES_NATIVE_SPATIAL_TYPES = tuple(
    int(part) for part in pyspark.__version__.split(".")[:2]
) >= (4, 2)

if USES_NATIVE_SPATIAL_TYPES:

    class LegacyGeometryType(GeometryType):
        """Explicit Sedona geometry UDT for native Spark schema resolution."""

    class LegacyGeographyType(GeographyType):
        """Explicit Sedona geography UDT for native Spark schema resolution."""

    from pyspark.sql.types import GeographyType, GeometryType
else:
    # Older Spark requires each value's registered __UDT__ to equal its schema.
    LegacyGeometryType = GeometryType
    LegacyGeographyType = GeographyType


def geometry_type():
    """Default geometry schema, allowing per-row SRIDs on Spark 4.2+."""
    return GeometryType("ANY") if USES_NATIVE_SPATIAL_TYPES else GeometryType()


def geography_type():
    """Default geography schema, allowing supported per-row SRIDs on Spark 4.2+."""
    return GeographyType("ANY") if USES_NATIVE_SPATIAL_TYPES else GeographyType()


def _native_wkb(geometry):
    import shapely

    if not hasattr(shapely, "to_wkb"):
        raise ImportError("Spark 4.2 spatial conversion requires Shapely 2 or later")
    return shapely.to_wkb(
        geometry,
        flavor="iso",
        include_srid=False,
        output_dimension=(
            4 if hasattr(shapely, "has_m") and shapely.geos_version >= (3, 12, 0) else 3
        ),
    )


def to_spark_geometry(geometry):
    """Convert Shapely to Spark's native value, preserving dimensions and SRID.

    Use this for ``createDataFrame`` and Python UDF results on Spark 4.2+.
    ``collect`` returns Spark Geometry values there; call ``to_shapely`` to
    obtain Shapely values. Older Spark accepts and returns Shapely directly.
    Native conversion requires Shapely 2+. ``None`` is preserved.
    """
    if geometry is None or not USES_NATIVE_SPATIAL_TYPES:
        return geometry
    from pyspark.sql.types import Geometry

    if isinstance(geometry, Geometry):
        return geometry
    wkb = _native_wkb(geometry)
    import shapely

    return Geometry.fromWKB(wkb, int(shapely.get_srid(geometry)))


def to_spark_geography(geometry):
    """Convert Shapely or Sedona Geography to Spark's native geography value.

    An unset Shapely SRID (0) becomes Spark geography's default 4326 on 4.2+.
    Other SRIDs are preserved for Spark's JVM boundary to validate. Older
    runtimes retain Sedona's Geography wrapper and SRID. Use an ANY schema
    where PySpark's CRS mapping cannot represent a supported non-default SRID.
    This conversion does not change Sedona's S2 computation semantics.
    """
    if geometry is None:
        return None
    if isinstance(geometry, Geography):
        if not USES_NATIVE_SPATIAL_TYPES:
            return geometry
        geometry = geometry.geometry
    if not USES_NATIVE_SPATIAL_TYPES:
        return Geography(geometry)
    from pyspark.sql.types import Geography as NativeGeography

    if isinstance(geometry, NativeGeography):
        return geometry
    wkb = _native_wkb(geometry)
    import shapely

    srid = int(shapely.get_srid(geometry)) or 4326
    return NativeGeography.fromWKB(wkb, srid)


def to_shapely(value):
    """Convert native/Sedona spatial values to Shapely, retaining SRID.

    Shapely values and ``None`` pass through. Native conversion needs Shapely 2+.
    """
    if isinstance(value, Geography):
        return value.geometry
    if USES_NATIVE_SPATIAL_TYPES:
        from pyspark.sql.types import Geography as NativeGeography, Geometry

        if isinstance(value, (Geometry, NativeGeography)):
            import shapely

            if not hasattr(shapely, "from_wkb"):
                raise ImportError(
                    "Spark 4.2 spatial conversion requires Shapely 2 or later"
                )
            return shapely.set_srid(
                shapely.from_wkb(bytes(value.getBytes())), value.getSrid()
            )
    return value


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
