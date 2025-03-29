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

import os
import shutil

import pytest
from tests.test_base import TestBase
from tests.tools import tests_resource

from sedona.core.enums import GridType, IndexType
from sedona.core.formatMapper.disc_utils import (
    GeoType,
    load_spatial_index_rdd_from_disc,
    load_spatial_rdd_from_disc,
)
from sedona.core.spatialOperator import JoinQuery
from sedona.core.SpatialRDD import LineStringRDD, PointRDD, PolygonRDD


def remove_directory(path: str) -> bool:
    try:
        shutil.rmtree(path)
    except Exception as e:
        return False
    return True


disc_location = os.path.join(tests_resource, "spatial_objects/temp")


class TestDiscUtils(TestBase):

    def test_saving_to_disc_spatial_rdd_point(self):
        from tests.properties.point_properties import (
            input_location,
            num_partitions,
            offset,
            splitter,
        )

        point_rdd = PointRDD(
            self.sc, input_location, offset, splitter, True, num_partitions
        )

        point_rdd.rawJvmSpatialRDD.saveAsObjectFile(
            os.path.join(disc_location, "point")
        )

    def test_saving_to_disc_spatial_rdd_polygon(self):
        from tests.properties.polygon_properties import (
            input_location,
            num_partitions,
            splitter,
        )

        polygon_rdd = PolygonRDD(
            self.sc, input_location, splitter, True, num_partitions
        )
        polygon_rdd.rawJvmSpatialRDD.saveAsObjectFile(
            os.path.join(disc_location, "polygon")
        )

    def test_saving_to_disc_spatial_rdd_linestring(self):
        from tests.properties.linestring_properties import (
            input_location,
            num_partitions,
            splitter,
        )

        linestring_rdd = LineStringRDD(
            self.sc, input_location, splitter, True, num_partitions
        )
        linestring_rdd.rawJvmSpatialRDD.saveAsObjectFile(
            os.path.join(disc_location, "line_string")
        )

    def test_saving_to_disc_index_linestring(self):
        from tests.properties.linestring_properties import (
            input_location,
            num_partitions,
            splitter,
        )

        linestring_rdd = LineStringRDD(
            self.sc, input_location, splitter, True, num_partitions
        )
        linestring_rdd.buildIndex(IndexType.RTREE, False)
        linestring_rdd.indexedRawRDD.saveAsObjectFile(
            os.path.join(disc_location, "line_string_index")
        )

    def test_saving_to_disc_index_polygon(self):
        from tests.properties.polygon_properties import (
            input_location,
            num_partitions,
            splitter,
        )

        polygon_rdd = PolygonRDD(
            self.sc, input_location, splitter, True, num_partitions
        )
        polygon_rdd.buildIndex(IndexType.RTREE, False)
        polygon_rdd.indexedRawRDD.saveAsObjectFile(
            os.path.join(disc_location, "polygon_index")
        )

    def test_saving_to_disc_index_point(self):
        from tests.properties.point_properties import (
            input_location,
            num_partitions,
            offset,
            splitter,
        )

        point_rdd = PointRDD(
            self.sc, input_location, offset, splitter, True, num_partitions
        )
        point_rdd.buildIndex(IndexType.RTREE, False)
        point_rdd.indexedRawRDD.saveAsObjectFile(
            os.path.join(disc_location, "point_index")
        )

    def test_loading_spatial_rdd_from_disc(self):
        point_rdd = load_spatial_rdd_from_disc(
            self.sc, os.path.join(disc_location, "point"), GeoType.POINT
        )
        point_index_rdd = load_spatial_index_rdd_from_disc(
            self.sc, os.path.join(disc_location, "point_index")
        )
        point_rdd.indexedRawRDD = point_index_rdd

        assert point_rdd.indexedRawRDD is not None
        assert isinstance(point_rdd, PointRDD)
        point_rdd.analyze()

        polygon_rdd = load_spatial_rdd_from_disc(
            self.sc, os.path.join(disc_location, "polygon"), GeoType.POLYGON
        )
        polygon_index_rdd = load_spatial_index_rdd_from_disc(
            self.sc, os.path.join(disc_location, "polygon_index")
        )
        polygon_rdd.indexedRawRDD = polygon_index_rdd
        polygon_rdd.analyze()

        assert polygon_rdd.indexedRawRDD is not None
        assert isinstance(polygon_rdd, PolygonRDD)

        linestring_rdd = load_spatial_rdd_from_disc(
            self.sc, os.path.join(disc_location, "line_string"), GeoType.LINESTRING
        )
        linestring_index_rdd = load_spatial_index_rdd_from_disc(
            self.sc, os.path.join(disc_location, "line_string_index")
        )
        linestring_rdd.indexedRawRDD = linestring_index_rdd

        assert linestring_rdd.indexedRawRDD is not None
        assert isinstance(linestring_rdd, LineStringRDD)

        linestring_rdd.analyze()

        linestring_rdd.spatialPartitioning(GridType.KDBTREE)
        polygon_rdd.spatialPartitioning(linestring_rdd.getPartitioner())
        polygon_rdd.buildIndex(IndexType.RTREE, True)
        linestring_rdd.buildIndex(IndexType.RTREE, True)

        result = JoinQuery.SpatialJoinQuery(
            linestring_rdd, polygon_rdd, True, True
        ).collect()

        remove_directory(disc_location)
