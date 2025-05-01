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
from sedona.spark.core.SpatialRDD import SpatialRDD
from sedona.spark.utils.meta import MultipleMeta


class WkbReader(GeoDataReader, metaclass=MultipleMeta):

    @classmethod
    def readToGeometryRDD(
        cls,
        sc: SparkContext,
        inputPath: str,
        wkbColumn: int,
        allowInvalidGeometries: bool,
        skipSyntacticallyInvalidGeometries: bool,
    ) -> SpatialRDD:
        """

        :param sc:
        :param inputPath:
        :param wkbColumn:
        :param allowInvalidGeometries:
        :param skipSyntacticallyInvalidGeometries:
        :return:
        """
        jvm = sc._jvm
        spatial_rdd = SpatialRDD(sc)
        srdd = jvm.WkbReader.readToGeometryRDD(
            sc._jsc,
            inputPath,
            wkbColumn,
            allowInvalidGeometries,
            skipSyntacticallyInvalidGeometries,
        )
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd

    @classmethod
    def readToGeometryRDD(
        cls,
        rawTextRDD: RDD,
        wkbColumn: int,
        allowInvalidGeometries: bool,
        skipSyntacticallyInvalidGeometries: bool,
    ) -> SpatialRDD:
        """

        :param rawTextRDD:
        :param wkbColumn:
        :param allowInvalidGeometries:
        :param skipSyntacticallyInvalidGeometries:
        :return:
        """
        sc = rawTextRDD.ctx
        jvm = sc._jvm

        spatial_rdd = SpatialRDD(sc)
        srdd = jvm.WkbReader.readToGeometryRDD(
            rawTextRDD._jrdd,
            wkbColumn,
            allowInvalidGeometries,
            skipSyntacticallyInvalidGeometries,
        )
        spatial_rdd.set_srdd(srdd)

        return spatial_rdd
