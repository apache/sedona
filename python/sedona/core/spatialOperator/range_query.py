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

from shapely.geometry.base import BaseGeometry

from sedona.core.SpatialRDD.spatial_rdd import SpatialRDD
from sedona.core.spatialOperator.range_query_raw import RangeQueryRaw
from sedona.utils.decorators import require


class RangeQuery:

    @classmethod
    @require(["RangeQuery", "GeometryAdapter", "GeoSerializerData"])
    def SpatialRangeQuery(self, spatialRDD: SpatialRDD, rangeQueryWindow: BaseGeometry,
                          considerBoundaryIntersection: bool, usingIndex: bool):
        """

        :param spatialRDD:
        :param rangeQueryWindow:
        :param considerBoundaryIntersection:
        :param usingIndex:
        :return:
        """
        j_srdd = RangeQueryRaw.SpatialRangeQuery(spatialRDD, rangeQueryWindow, considerBoundaryIntersection, usingIndex)
        return j_srdd.to_rdd()
