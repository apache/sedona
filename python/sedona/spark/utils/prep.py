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

from typing import List

from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)
from shapely.geometry.base import BaseGeometry

from sedona.spark.core.geom.geography import Geography


def assign_all() -> bool:
    geoms = [
        Point,
        MultiPoint,
        Polygon,
        MultiPolygon,
        LineString,
        MultiLineString,
        GeometryCollection,
    ]
    assign_udt_shapely_objects(geoms=geoms)
    assign_user_data_to_shapely_objects(geoms=geoms)
    assign_udt_geography()
    assign_udt_raster()
    return True


def assign_udt_shapely_objects(geoms: List[type(BaseGeometry)]) -> bool:
    from sedona.spark.sql.types import GeometryType

    for geom in geoms:
        geom.__UDT__ = GeometryType()
    return True


def assign_user_data_to_shapely_objects(geoms: List[type(BaseGeometry)]) -> bool:
    for geom in geoms:
        geom.getUserData = lambda geom_instance: geom_instance.userData


def assign_udt_geography():
    from sedona.spark.sql.types import GeographyType

    Geography.__UDT__ = GeographyType()


def assign_udt_raster():
    from sedona.spark.sql.types import RasterType

    try:
        from sedona.spark.raster.sedona_raster import SedonaRaster

        SedonaRaster.__UDT__ = RasterType()
    except ImportError:
        # Some dependencies for SedonaRaster to work is not available (for
        # instance, rasterio), Raster support is an opt-in feature, so we
        # ignore this error and skip registering UDT for it.
        pass
