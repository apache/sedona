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

from abc import ABC

import attr
from pyspark import SparkContext

from sedona.utils.decorators import require


@attr.s
class SpatialRDDFactory(ABC):
    sparkContext = attr.ib(type=SparkContext)

    def __attrs_post_init__(self):
        self._jvm = self.sparkContext._jvm

    @require(["PointRDD"])
    def create_point_rdd(self):
        return self._jvm.PointRDD

    @require(["PolygonRDD"])
    def create_polygon_rdd(self):
        return self._jvm.PolygonRDD

    @require(["LineStringRDD"])
    def create_linestring_rdd(self):
        return self._jvm.LineStringRDD

    @require(["RectangleRDD"])
    def create_rectangle_rdd(self):
        return self._jvm.RectangleRDD

    @require(["CircleRDD"])
    def create_circle_rdd(self):
        return self._jvm.CircleRDD

    @require(["SpatialRDD"])
    def create_spatial_rdd(self):
        return self._jvm.SpatialRDD
