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

from pyspark import RDD

from sedona.core.SpatialRDD.spatial_rdd import SpatialRDD
from sedona.core.jvm.translate import JvmSedonaPythonConverter
from sedona.core.spatialOperator.join_params import JoinParams
from sedona.utils.decorators import require
from sedona.utils.spatial_rdd_parser import SedonaPickler


class JoinQuery:

    @classmethod
    @require(["JoinQuery"])
    def SpatialJoinQuery(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool, considerBoundaryIntersection: bool) -> RDD:
        """

        :param spatialRDD: SpatialRDD
        :param queryRDD: SpatialRDD
        :param useIndex: bool
        :param considerBoundaryIntersection: bool
        :return:
        """

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc

        srdd = jvm.JoinQuery.SpatialJoinQuery(
            spatialRDD._srdd,
            queryRDD._srdd,
            useIndex,
            considerBoundaryIntersection
        )
        serialized = JvmSedonaPythonConverter(jvm)\
            .translate_spatial_pair_rdd_with_hashset_to_python(srdd)

        return RDD(serialized, sc, SedonaPickler())

    @classmethod
    @require(["JoinQuery"])
    def DistanceJoinQuery(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool, considerBoundaryIntersection: bool) -> RDD:
        """

        :param spatialRDD: SpatialRDD
        :param queryRDD: SpatialRDD
        :param useIndex: bool
        :param considerBoundaryIntersection: bool
        :return:
        """

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc
        srdd = jvm.JoinQuery.DistanceJoinQuery(
            spatialRDD._srdd,
            queryRDD._srdd,
            useIndex,
            considerBoundaryIntersection
        )
        serialized = JvmSedonaPythonConverter(jvm).\
            translate_spatial_pair_rdd_with_hashset_to_python(srdd)

        return RDD(serialized, sc, SedonaPickler())

    @classmethod
    @require(["JoinQuery"])
    def spatialJoin(cls, queryWindowRDD: SpatialRDD, objectRDD: SpatialRDD, joinParams: JoinParams) -> RDD:
        """

        :param queryWindowRDD: SpatialRDD
        :param objectRDD: SpatialRDD
        :param joinParams: JoinParams
        :return:
        """

        jvm = queryWindowRDD._jvm
        sc = queryWindowRDD._sc

        jvm_join_params = joinParams.jvm_instance(jvm)

        srdd = jvm.JoinQuery.spatialJoin(queryWindowRDD._srdd, objectRDD._srdd, jvm_join_params)
        serialized = JvmSedonaPythonConverter(jvm).\
            translate_spatial_pair_rdd_to_python(srdd)

        return RDD(serialized, sc, SedonaPickler())

    @classmethod
    @require(["JoinQuery"])
    def DistanceJoinQueryFlat(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool, considerBoundaryIntersection: bool) -> RDD:
        """

        :param spatialRDD: SpatialRDD
        :param queryRDD: SpatialRDD
        :param useIndex: bool
        :param considerBoundaryIntersection: bool

        >> spatial_rdd =
        >> query_rdd =
        >> spatial_join_result = JoinQuery.DistanceJoinQueryFlat(spatial_rdd, query_rdd, True, True)
        >> spatial_join_result.collect()
        [GeoData(), GeoData()]
        :return:
        """

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc

        spatial_join = jvm.JoinQuery.DistanceJoinQueryFlat
        srdd = spatial_join(
            spatialRDD._srdd,
            queryRDD._srdd,
            useIndex,
            considerBoundaryIntersection
        )

        serialized = JvmSedonaPythonConverter(jvm).\
            translate_spatial_pair_rdd_to_python(srdd)

        return RDD(serialized, sc, SedonaPickler())

    @classmethod
    @require(["JoinQuery"])
    def SpatialJoinQueryFlat(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool,
                              considerBoundaryIntersection: bool) -> RDD:
        """
        Function takes SpatialRDD and other SpatialRDD and based on two parameters
        - useIndex
        - considerBoundaryIntersection
        creates RDD with result of Spatial Join operation. It Returns RDD[GeoData, GeoData]

        :param spatialRDD: SpatialRDD
        :param queryRDD: SpatialRDD
        :param useIndex: bool
        :param considerBoundaryIntersection: bool
        :return: RDD

        >> spatial_join_result = JoinQuery.SpatialJoinQueryFlat(
        >>      spatialRDD, queryRDD, useIndex, considerBoundaryIntersection
        >> )
        >> spatial_join_result.collect()
        [[GeoData(Polygon, ), GeoData()], [GeoData(), GeoData()], [GeoData(), GeoData()]]
        """

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc

        spatial_join = jvm.JoinQuery.SpatialJoinQueryFlat
        srdd = spatial_join(
            spatialRDD._srdd,
            queryRDD._srdd,
            useIndex,
            considerBoundaryIntersection
        )

        serialized = JvmSedonaPythonConverter(jvm).\
            translate_spatial_pair_rdd_to_python(srdd)

        return RDD(serialized, sc, SedonaPickler())
