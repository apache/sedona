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

from pyspark import SparkContext

from sedona.core.SpatialRDD import SpatialRDD, PolygonRDD, LineStringRDD, PointRDD
from sedona.core.jvm.translate import SpatialObjectLoaderAdapter
from sedona.utils.decorators import require


class DiscLoader:

    @classmethod
    def load(cls, sc: SparkContext, path: str) -> SpatialRDD:
        raise NotImplementedError()


class IndexDiscLoader:

    @classmethod
    def load(cls, sc: SparkContext, path: str):
        jvm = sc._jvm
        index_rdd = SpatialObjectLoaderAdapter(jvm).load_index_rdd(sc._jsc, path)
        return index_rdd


class PolygonRDDDiscLoader(DiscLoader):

    @classmethod
    @require(["SpatialObjectLoaderAdapter"])
    def load(cls, sc: SparkContext, path: str) -> SpatialRDD:
        jvm = sc._jvm
        polygon_rdd = PolygonRDD()
        srdd = SpatialObjectLoaderAdapter(jvm).load_polygon_spatial_rdd(sc._jsc, path)
        polygon_rdd.set_srdd(srdd)
        return polygon_rdd


class PointRDDDiscLoader(DiscLoader):

    @classmethod
    @require(["SpatialObjectLoaderAdapter"])
    def load(cls, sc: SparkContext, path: str) -> SpatialRDD:
        jvm = sc._jvm
        point_rdd = PointRDD()
        srdd = SpatialObjectLoaderAdapter(jvm).load_point_spatial_rdd(sc._jsc, path)
        point_rdd.set_srdd(srdd)
        return point_rdd


class LineStringRDDDiscLoader(DiscLoader):

    @classmethod
    @require(["SpatialObjectLoaderAdapter"])
    def load(cls, sc: SparkContext, path: str) -> SpatialRDD:
        jvm = sc._jvm
        line_string_rdd = LineStringRDD()
        srdd = SpatialObjectLoaderAdapter(jvm).load_line_string_spatial_rdd(sc._jsc, path)
        line_string_rdd.set_srdd(srdd)
        return line_string_rdd


class SpatialRDDDiscLoader(DiscLoader):

    @classmethod
    @require(["SpatialObjectLoaderAdapter"])
    def load(cls, sc: SparkContext, path: str) -> SpatialRDD:
        jvm = sc._jvm
        spatial_rdd = SpatialRDD()
        srdd = SpatialObjectLoaderAdapter(jvm).load_spatial_rdd(sc._jsc, path)
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd


class GeoType(Enum):
    POINT = "POINT"
    POLYGON = "POLYGON"
    LINESTRING = "LINESTRING"
    GEOMETRY = "GEOMETRY"


loaders = {
    GeoType.POINT: PointRDDDiscLoader,
    GeoType.POLYGON: PolygonRDDDiscLoader,
    GeoType.LINESTRING: LineStringRDDDiscLoader,
    GeoType.GEOMETRY: SpatialRDDDiscLoader
}


def load_spatial_rdd_from_disc(sc: SparkContext, path: str, geometry_type: GeoType):
    """

    :param sc:
    :param path:
    :param geometry_type:
    :return:
    """
    return loaders[geometry_type].load(sc, path)


def load_spatial_index_rdd_from_disc(sc: SparkContext, path: str):
    return IndexDiscLoader.load(sc, path)
