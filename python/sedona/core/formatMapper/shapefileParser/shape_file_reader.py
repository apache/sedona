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

import attr
from pyspark import SparkContext

from sedona.core.SpatialRDD import PolygonRDD, PointRDD, LineStringRDD
from sedona.core.SpatialRDD.spatial_rdd import SpatialRDD
from sedona.core.formatMapper.geo_reader import GeoDataReader
from sedona.utils.meta import MultipleMeta


@attr.s
class ShapefileReader(GeoDataReader, metaclass=MultipleMeta):

    @classmethod
    def readToGeometryRDD(cls, sc: SparkContext, inputPath: str) -> SpatialRDD:
        """

        :param sc:
        :param inputPath:
        :return:
        """
        jvm = sc._jvm
        jsc = sc._jsc
        srdd = jvm.ShapefileReader.readToGeometryRDD(
            jsc,
            inputPath
        )
        spatial_rdd = SpatialRDD(sc=sc)

        spatial_rdd.set_srdd(srdd)
        return spatial_rdd

    @classmethod
    def readToPolygonRDD(cls, sc: SparkContext, inputPath: str) -> PolygonRDD:
        """

        :param sc:
        :param inputPath:
        :return:
        """
        jvm = sc._jvm
        jsc = sc._jsc
        srdd = jvm.ShapefileReader.readToPolygonRDD(
            jsc,
            inputPath
        )
        spatial_rdd = PolygonRDD()
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd

    @classmethod
    def readToPointRDD(cls, sc: SparkContext, inputPath: str) -> PointRDD:
        """

        :param sc:
        :param inputPath:
        :return:
        """
        jvm = sc._jvm
        jsc = sc._jsc
        srdd = jvm.ShapefileReader.readToPointRDD(
            jsc,
            inputPath
        )
        spatial_rdd = PointRDD()
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd

    @classmethod
    def readToLineStringRDD(cls, sc: SparkContext, inputPath: str) -> LineStringRDD:
        """

        :param sc:
        :param inputPath:
        :return:
        """
        jvm = sc._jvm
        jsc = sc._jsc
        srdd = jvm.ShapefileReader.readToLineStringRDD(
            jsc,
            inputPath
        )
        spatial_rdd = LineStringRDD()
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd
