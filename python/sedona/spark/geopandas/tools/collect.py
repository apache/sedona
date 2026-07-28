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

import geopandas as gpd
import shapely
from packaging.version import parse as parse_version
from pyspark.sql import functions as F

from sedona.spark.sql import st_aggregates as sta
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf

SHAPELY_LT_20 = parse_version(shapely.__version__) < parse_version("2.0.0")


def collect(x, multi=False):
    """Collect single-part geometries into their multipart counterpart.

    A Sedona :class:`~sedona.spark.geopandas.GeoSeries` is aggregated on
    executors with ``ST_Collect_Agg``. One aggregate action materializes only
    the metadata and single geometry required by this scalar-returning API on
    the driver. Local Shapely and GeoPandas inputs retain GeoPandas' behavior.

    Parameters
    ----------
    x : GeoSeries, iterable, or Shapely geometry
        Homogeneous geometries to collect.
    multi : bool, default False
        Force a singleton single-part geometry into its multipart type.

    Returns
    -------
    shapely.geometry.base.BaseGeometry
        A singleton geometry or its collected multipart geometry.

    Raises
    ------
    ValueError
        If geometry types differ, or multiple multipart geometries are passed.

    Examples
    --------
    >>> from shapely.geometry import Point
    >>> from sedona.spark.geopandas import GeoSeries
    >>> from sedona.spark.geopandas.tools import collect
    >>> collect(GeoSeries([Point(0, 0), Point(1, 1)]))
    <MULTIPOINT ((0 0), (1 1))>
    >>> collect(Point(0, 0), multi=True)
    <MULTIPOINT ((0 0))>
    """
    from sedona.spark.geopandas import GeoSeries

    if not isinstance(x, GeoSeries):
        return gpd.tools.collect(x, multi=multi)

    geometry = x.spark.column
    geometry_type = stf.ST_GeometryType(geometry)
    geometry_is_empty = stf.ST_IsEmpty(geometry)
    if SHAPELY_LT_20:
        # Shapely 1 reports every empty geometry as GeometryCollection,
        # regardless of whether the serializer retains its original family.
        geometry_type = F.when(
            geometry_is_empty,
            F.lit("ST_GeometryCollection"),
        ).otherwise(geometry_type)
    non_empty_geometry = F.when(~geometry_is_empty, geometry)
    first_geometry_type = F.first(geometry_type, ignorenulls=True)
    empty_multipolygon = stc.ST_GeomFromWKT(
        F.lit("MULTIPOLYGON EMPTY"),
        F.first(stf.ST_SRID(geometry), ignorenulls=True),
    )
    collected_geometry = F.coalesce(
        sta.ST_Collect_Agg(non_empty_geometry),
        F.when(
            first_geometry_type == F.lit("ST_Polygon"),
            empty_multipolygon,
        ),
    )
    metadata = x._internal.spark_frame.agg(
        F.count(F.lit(1)).alias("__collect_count__"),
        F.count(geometry).alias("__collect_non_null_count__"),
        F.count(non_empty_geometry).alias("__collect_non_empty_count__"),
        F.countDistinct(geometry_type).alias("__collect_type_count__"),
        first_geometry_type.alias("__collect_type__"),
        F.first(geometry, ignorenulls=True).alias("__collect_single_geometry__"),
        collected_geometry.alias("__collect_geometry__"),
    ).first()

    count = metadata["__collect_count__"]
    non_null_count = metadata["__collect_non_null_count__"]
    if count == 0:
        raise IndexError("list index out of range")
    if non_null_count != count:
        raise AttributeError("'NoneType' object has no attribute 'geom_type'")
    if metadata["__collect_type_count__"] != 1:
        raise ValueError("Geometry type must be homogeneous")

    geometry_type_name = metadata["__collect_type__"]
    if geometry_type_name.startswith("ST_"):
        geometry_type_name = geometry_type_name[3:]

    is_multi = geometry_type_name.startswith("Multi")
    if count > 1 and is_multi:
        raise ValueError(
            f"Cannot collect {geometry_type_name}. Must have single geometries"
        )

    if count == 1 and (is_multi or not multi):
        return metadata["__collect_single_geometry__"]

    if geometry_type_name not in {"Point", "LineString", "Polygon"}:
        raise KeyError(geometry_type_name)

    non_empty_count = metadata["__collect_non_empty_count__"]
    has_empty = non_empty_count != count
    if has_empty and geometry_type_name in {
        "Point",
        "LineString",
    }:
        # EmptyPartError was added after Shapely 1.7.0, Sedona's supported
        # dependency floor. Keep importing this module safe on that version.
        try:
            from shapely.errors import EmptyPartError
        except ImportError:
            raise ValueError(
                f"Can't create Multi{geometry_type_name} with empty component"
            ) from None
        raise EmptyPartError(
            f"Can't create Multi{geometry_type_name} with empty component"
        )

    return metadata["__collect_geometry__"]
