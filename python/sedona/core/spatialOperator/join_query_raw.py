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

from sedona.core.SpatialRDD import SpatialRDD
from sedona.core.spatialOperator.join_params import JoinParams
from sedona.core.spatialOperator.rdd import SedonaPairRDDList, SedonaPairRDD
from sedona.utils.decorators import require


class JoinQueryRaw:

    @classmethod
    @require(["JoinQuery"])
    def SpatialJoinQuery(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool, considerBoundaryIntersection: bool) -> SedonaPairRDDList:
        jvm = spatialRDD._jvm
        sc = spatialRDD._sc

        srdd = jvm.JoinQuery.SpatialJoinQuery(
            spatialRDD._srdd,
            queryRDD._srdd,
            useIndex,
            considerBoundaryIntersection
        )

        return SedonaPairRDDList(srdd, sc)

    @classmethod
    @require(["JoinQuery"])
    def DistanceJoinQuery(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool, considerBoundaryIntersection: bool) -> SedonaPairRDDList:

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc
        srdd = jvm.JoinQuery.DistanceJoinQuery(
            spatialRDD._srdd,
            queryRDD._srdd,
            useIndex,
            considerBoundaryIntersection
        )
        return SedonaPairRDDList(srdd, sc)

    @classmethod
    @require(["JoinQuery"])
    def spatialJoin(cls, queryWindowRDD: SpatialRDD, objectRDD: SpatialRDD, joinParams: JoinParams) -> SedonaPairRDD:

        jvm = queryWindowRDD._jvm
        sc = queryWindowRDD._sc

        jvm_join_params = joinParams.jvm_instance(jvm)

        srdd = jvm.JoinQuery.spatialJoin(queryWindowRDD._srdd, objectRDD._srdd, jvm_join_params)

        return SedonaPairRDD(srdd, sc)

    @classmethod
    @require(["JoinQuery"])
    def DistanceJoinQueryFlat(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool, considerBoundaryIntersection: bool) -> SedonaPairRDD:

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc

        spatial_join = jvm.JoinQuery.DistanceJoinQueryFlat
        srdd = spatial_join(
            spatialRDD._srdd,
            queryRDD._srdd,
            useIndex,
            considerBoundaryIntersection
        )
        return SedonaPairRDD(srdd, sc)

    @classmethod
    @require(["JoinQuery"])
    def SpatialJoinQueryFlat(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool,
                             considerBoundaryIntersection: bool) -> SedonaPairRDD:

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc

        spatial_join = jvm.JoinQuery.SpatialJoinQueryFlat
        srdd = spatial_join(
            spatialRDD._srdd,
            queryRDD._srdd,
            useIndex,
            considerBoundaryIntersection
        )
        return SedonaPairRDD(srdd, sc)
