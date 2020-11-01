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

from sedona.core.SpatialRDD.spatial_rdd import SpatialRDD
from sedona.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory
from sedona.utils.meta import MultipleMeta


class CircleRDD(SpatialRDD, metaclass=MultipleMeta):

    def __init__(self, spatialRDD: SpatialRDD, Radius: float):
        """

        :param spatialRDD: SpatialRDD
        :param Radius: float
        """
        super()._do_init(spatialRDD._sc)
        self._srdd = self._jvm_spatial_rdd(
            spatialRDD._srdd,
            Radius
        )

    def getCenterPointAsSpatialRDD(self) -> 'PointRDD':
        from sedona.core.SpatialRDD import PointRDD
        srdd = self._srdd.getCenterPointAsSpatialRDD()
        point_rdd = PointRDD()
        point_rdd.set_srdd(srdd)
        return point_rdd

    def getCenterPolygonAsSpatialRDD(self) -> 'PolygonRDD':
        from sedona.core.SpatialRDD import PolygonRDD
        srdd = self._srdd.getCenterPolygonAsSpatialRDD()
        polygon_rdd = PolygonRDD()
        polygon_rdd.set_srdd(srdd)
        return polygon_rdd

    def getCenterLineStringRDDAsSpatialRDD(self) -> 'LineStringRDD':
        from sedona.core.SpatialRDD import LineStringRDD
        srdd = self._srdd.getCenterPolygonAsSpatialRDD()
        linestring_rdd = LineStringRDD()
        linestring_rdd.set_srdd(srdd)
        return linestring_rdd

    def getCenterRectangleRDDAsSpatialRDD(self) -> 'RectangleRDD':
        from sedona.core.SpatialRDD import RectangleRDD
        srdd = self._srdd.getCenterLineStringRDDAsSpatialRDD()
        rectangle_rdd = RectangleRDD()
        rectangle_rdd.set_srdd(srdd)
        return rectangle_rdd

    @property
    def _jvm_spatial_rdd(self):
        spatial_factory = SpatialRDDFactory(self._sc)
        return spatial_factory.create_circle_rdd()

    def MinimumBoundingRectangle(self):
        raise NotImplementedError("CircleRDD has not MinimumBoundingRectangle method.")
