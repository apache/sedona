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

from pyspark import RDD, SparkContext

from sedona.spark.core.formatMapper.geo_reader import GeoDataReader
from sedona.spark.core.SpatialRDD.spatial_rdd import SpatialRDD
from sedona.spark.utils.meta import MultipleMeta


class WktReader(GeoDataReader, metaclass=MultipleMeta):

    @classmethod
    def readToGeometryRDD(
        cls,
        sc: SparkContext,
        inputPath: str,
        wktColumn: int,
        allowInvalidGeometries: bool,
        skipSyntacticallyInvalidGeometries: bool,
    ) -> SpatialRDD:
        """

        :param sc: SparkContext
        :param inputPath: str
        :param wktColumn: int
        :param allowInvalidGeometries: bool
        :param skipSyntacticallyInvalidGeometries: bool
        :return:
        """
        jvm = sc._jvm
        srdd = jvm.WktReader.readToGeometryRDD(
            sc._jsc,
            inputPath,
            wktColumn,
            allowInvalidGeometries,
            skipSyntacticallyInvalidGeometries,
        )

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd

    @classmethod
    def readToGeometryRDD(
        cls,
        rawTextRDD: RDD,
        wktColumn: int,
        allowInvalidGeometries: bool,
        skipSyntacticallyInvalidGeometries: bool,
    ) -> SpatialRDD:
        """

        :param rawTextRDD: RDD
        :param wktColumn: int
        :param allowInvalidGeometries: bool
        :param skipSyntacticallyInvalidGeometries: bool
        :return:
        """
        sc = rawTextRDD.ctx
        jvm = sc._jvm
        srdd = jvm.WktReader.readToGeometryRDD(
            rawTextRDD._jrdd,
            wktColumn,
            allowInvalidGeometries,
            skipSyntacticallyInvalidGeometries,
        )
        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd
