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

from shapely.geometry.base import BaseGeometry

from sedona.spark.core.spatialOperator.rdd import SedonaRDD
from sedona.spark.core.SpatialRDD.spatial_rdd import SpatialRDD
from sedona.spark.utils.decorators import require
from sedona.spark.utils.geometry_adapter import GeometryAdapter


class RangeQueryRaw:

    @classmethod
    @require(["RangeQuery", "GeometryAdapter", "GeoSerializerData"])
    def SpatialRangeQuery(
        self,
        spatialRDD: SpatialRDD,
        rangeQueryWindow: BaseGeometry,
        considerBoundaryIntersection: bool,
        usingIndex: bool,
    ) -> SedonaRDD:
        """

        :param spatialRDD:
        :param rangeQueryWindow:
        :param considerBoundaryIntersection:
        :param usingIndex:
        :return:
        """

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc

        jvm_geom = GeometryAdapter.create_jvm_geometry_from_base_geometry(
            jvm, rangeQueryWindow
        )

        srdd = jvm.RangeQuery.SpatialRangeQuery(
            spatialRDD._srdd, jvm_geom, considerBoundaryIntersection, usingIndex
        )

        return SedonaRDD(srdd, sc)
