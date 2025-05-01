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

from shapely.geometry import LineString, Point, Polygon
from shapely.geometry.base import BaseGeometry

from sedona.spark.utils.spatial_rdd_parser import GeoData
from tests.test_base import TestBase

from sedona.spark.core.enums import GridType, IndexType
from sedona.spark.core.spatialOperator import JoinQuery
from sedona.spark.core.SpatialRDD import CircleRDD, LineStringRDD, PointRDD, PolygonRDD
from sedona.spark.core.SpatialRDD.spatial_rdd import SpatialRDD


class TestJoinQueryCorrectness(TestBase):

    def test_initial(self):
        self.once_before_all()

    def test_inside_point_join_correctness(self):
        self.once_before_all()

        window_rdd = PolygonRDD(self.sc.parallelize(self.test_polygon_window_set))

        object_rdd = PointRDD(self.sc.parallelize(self.test_inside_point_set))
        self.prepare_rdd(object_rdd, window_rdd, GridType.QUADTREE)

        result = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, True, False
        ).collect()
        self.verify_join_result(result)

        result_no_index = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, False, False
        ).collect()
        self.verify_join_result(result_no_index)

    def test_on_boundary_point_join_correctness(self):
        window_rdd = PolygonRDD(self.sc.parallelize(self.test_polygon_window_set))
        object_rdd = PointRDD(self.sc.parallelize(self.test_on_boundary_point_set))
        self.prepare_rdd(object_rdd, window_rdd, GridType.QUADTREE)

        result = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, True, False
        ).collect()
        self.verify_join_result(result)

        result_no_index = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, False, False
        ).collect()
        self.verify_join_result(result_no_index)

    def test_outside_point_join_correctness(self):
        self.once_before_all()
        window_rdd = PolygonRDD(self.sc.parallelize(self.test_polygon_window_set))
        object_rdd = PointRDD(self.sc.parallelize(self.test_outside_point_set))
        self.prepare_rdd(object_rdd, window_rdd, GridType.QUADTREE)

        result = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, True, False
        ).collect()
        assert 0 == result.__len__()

        result_no_index = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, False, False
        ).collect()
        assert 0 == result_no_index.__len__()

    def test_inside_linestring_join_correctness(self):
        window_rdd = PolygonRDD(self.sc.parallelize(self.test_polygon_window_set))
        object_rdd = LineStringRDD(self.sc.parallelize(self.test_inside_linestring_set))

        self.prepare_rdd(object_rdd, window_rdd, GridType.QUADTREE)

        result = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, True, False
        ).collect()
        self.verify_join_result(result)

        result_no_index = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, False, False
        ).collect()
        self.verify_join_result(result_no_index)

    def test_overlapped_linestring_join_correctness(self):
        window_rdd = PolygonRDD(self.sc.parallelize(self.test_polygon_window_set))
        object_rdd = LineStringRDD(
            self.sc.parallelize(self.test_overlapped_linestring_set)
        )
        self.prepare_rdd(object_rdd, window_rdd, GridType.QUADTREE)

        result = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, True, True
        ).collect()
        self.verify_join_result(result)

        result_no_index = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, False, True
        ).collect()
        self.verify_join_result(result_no_index)

    def test_outside_line_string_join_correctness(self):
        window_rdd = PolygonRDD(self.sc.parallelize(self.test_polygon_window_set))
        object_rdd = LineStringRDD(
            self.sc.parallelize(self.test_outside_linestring_set)
        )
        self.prepare_rdd(object_rdd, window_rdd, GridType.QUADTREE)

        result = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, True, False
        ).collect()
        assert 0 == result.__len__()

        result_no_index = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, False, False
        ).collect()
        assert 0 == result_no_index.__len__()

    def test_inside_polygon_join_correctness(self):
        window_rdd = PolygonRDD(self.sc.parallelize(self.test_polygon_window_set))

        object_rdd = PolygonRDD(self.sc.parallelize(self.test_inside_polygon_set))
        self.prepare_rdd(object_rdd, window_rdd, GridType.QUADTREE)

        result = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, True, False
        ).collect()
        self.verify_join_result(result)

        result_no_index = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, False, False
        ).collect()
        self.verify_join_result(result_no_index)

    def test_overlapped_polygon_join_correctness(self):
        window_rdd = PolygonRDD(self.sc.parallelize(self.test_polygon_window_set))
        object_rdd = PolygonRDD(self.sc.parallelize(self.test_overlapped_polygon_set))
        self.prepare_rdd(object_rdd, window_rdd, GridType.QUADTREE)

        result = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, True, True
        ).collect()
        self.verify_join_result(result)

        result_no_index = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, False, True
        ).collect()
        self.verify_join_result(result_no_index)

    def test_outside_polygon_join_correctness(self):
        window_rdd = PolygonRDD(self.sc.parallelize(self.test_polygon_window_set))
        object_rdd = PolygonRDD(self.sc.parallelize(self.test_outside_polygon_set))
        self.prepare_rdd(object_rdd, window_rdd, GridType.QUADTREE)

        result = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, True, False
        ).collect()
        assert 0 == result.__len__()

        result_no_index = JoinQuery.SpatialJoinQuery(
            object_rdd, window_rdd, False, False
        ).collect()
        assert 0 == result_no_index.__len__()

    def test_inside_polygon_distance_join_correctness(self):
        center_geometry_rdd = PolygonRDD(
            self.sc.parallelize(self.test_polygon_window_set)
        )
        window_rdd = CircleRDD(center_geometry_rdd, 0.1)
        object_rdd = PolygonRDD(self.sc.parallelize(self.test_inside_polygon_set))
        self.prepare_rdd(object_rdd, window_rdd, GridType.QUADTREE)

        result = JoinQuery.DistanceJoinQuery(
            object_rdd, window_rdd, True, False
        ).collect()
        self.verify_join_result(result)

        result_no_index = JoinQuery.DistanceJoinQuery(
            object_rdd, window_rdd, False, False
        ).collect()
        self.verify_join_result(result_no_index)

    def test_overlapped_polygon_distance_join_correctness(self):
        center_geometry_rdd = PolygonRDD(
            self.sc.parallelize(self.test_polygon_window_set)
        )
        window_rdd = CircleRDD(center_geometry_rdd, 0.1)
        object_rdd = PolygonRDD(self.sc.parallelize(self.test_overlapped_polygon_set))
        self.prepare_rdd(object_rdd, window_rdd, GridType.QUADTREE)

        result = JoinQuery.DistanceJoinQuery(
            object_rdd, window_rdd, True, True
        ).collect()
        self.verify_join_result(result)

        result_no_index = JoinQuery.DistanceJoinQuery(
            object_rdd, window_rdd, False, True
        ).collect()
        self.verify_join_result(result_no_index)

    def test_outside_polygon_distance_join_correctness(self):
        center_geometry_rdd = PolygonRDD(
            self.sc.parallelize(self.test_polygon_window_set)
        )
        window_rdd = CircleRDD(center_geometry_rdd, 0.1)
        object_rdd = PolygonRDD(self.sc.parallelize(self.test_outside_polygon_set))
        self.prepare_rdd(object_rdd, window_rdd, GridType.QUADTREE)

        result = JoinQuery.DistanceJoinQuery(
            object_rdd, window_rdd, True, True
        ).collect()
        assert 0 == result.__len__()

        result_no_index = JoinQuery.DistanceJoinQuery(
            object_rdd, window_rdd, False, True
        ).collect()
        assert 0 == result_no_index.__len__()

    def prepare_rdd(
        self, object_rdd: SpatialRDD, window_rdd: SpatialRDD, grid_type: GridType
    ):
        object_rdd.analyze()
        window_rdd.analyze()
        object_rdd.rawSpatialRDD.repartition(4)
        object_rdd.spatialPartitioning(grid_type)
        object_rdd.buildIndex(IndexType.RTREE, True)
        window_rdd.spatialPartitioning(object_rdd.getPartitioner())

    @classmethod
    def verify_join_result(cls, result):
        assert result.__len__() == 200

    @classmethod
    def make_square(cls, minx: float, miny: float, side: float) -> Polygon:
        coordinates = [
            (minx, miny),
            (minx + side, miny),
            (minx + side, miny + side),
            (minx, miny + side),
        ]

        polygon = Polygon(coordinates)

        return polygon

    @classmethod
    def make_square_line(cls, minx: float, miny: float, side: float):
        coordinates = [(minx, miny), (minx + side, miny), (minx + side, miny + side)]
        return LineString(coordinates)

    @classmethod
    def make_point(cls, x: float, y: float):
        return Point(x, y)

    @classmethod
    def wrap(cls, geom: BaseGeometry, user_data: str):
        return GeoData(geom=geom, userData=user_data)

    @classmethod
    def once_before_all(cls):
        cls.test_polygon_window_set = []
        cls.test_inside_polygon_set = []
        cls.test_overlapped_polygon_set = []
        cls.test_outside_polygon_set = []
        cls.test_inside_linestring_set = []
        cls.test_overlapped_linestring_set = []
        cls.test_outside_linestring_set = []
        cls.test_inside_point_set = []
        cls.test_on_boundary_point_set = []
        cls.test_outside_point_set = []

        for base_x in range(0, 100, 10):
            for base_y in range(0, 100, 10):
                id = str(base_x) + ":" + str(base_y)
                a = "a:" + id
                b = "b:" + id

                cls.test_polygon_window_set.append(
                    cls.wrap(cls.make_square(base_x, base_y, 5), a)
                )
                cls.test_polygon_window_set.append(
                    cls.wrap(cls.make_square(base_x, base_y, 5), b)
                )

                cls.test_inside_polygon_set.append(
                    cls.wrap(cls.make_square(base_x + 2, base_y + 2, 2), a)
                )
                cls.test_inside_polygon_set.append(
                    cls.wrap(cls.make_square(base_x + 2, base_y + 2, 2), b)
                )

                cls.test_overlapped_polygon_set.append(
                    cls.wrap(cls.make_square(base_x + 3, base_y + 3, 3), a)
                )
                cls.test_overlapped_polygon_set.append(
                    cls.wrap(cls.make_square(base_x + 3, base_y + 3, 3), b)
                )

                cls.test_outside_polygon_set.append(
                    cls.wrap(cls.make_square(base_x + 6, base_y + 6, 3), a)
                )
                cls.test_outside_polygon_set.append(
                    cls.wrap(cls.make_square(base_x + 6, base_y + 6, 3), b)
                )

                cls.test_inside_linestring_set.append(
                    cls.wrap(cls.make_square_line(base_x + 2, base_y + 2, 2), a)
                )
                cls.test_inside_linestring_set.append(
                    cls.wrap(cls.make_square_line(base_x + 2, base_y + 2, 2), b)
                )

                cls.test_overlapped_linestring_set.append(
                    cls.wrap(cls.make_square_line(base_x + 3, base_y + 3, 3), a)
                )
                cls.test_overlapped_linestring_set.append(
                    cls.wrap(cls.make_square_line(base_x + 3, base_y + 3, 3), b)
                )

                cls.test_outside_linestring_set.append(
                    cls.wrap(cls.make_square_line(base_x + 6, base_y + 6, 3), a)
                )
                cls.test_outside_linestring_set.append(
                    cls.wrap(cls.make_square_line(base_x + 6, base_y + 6, 3), b)
                )

                cls.test_inside_point_set.append(
                    cls.wrap(cls.make_point(base_x + 2.5, base_y + 2.5), a)
                )
                cls.test_inside_point_set.append(
                    cls.wrap(cls.make_point(base_x + 2.5, base_y + 2.5), b)
                )

                cls.test_on_boundary_point_set.append(
                    cls.wrap(cls.make_point(base_x + 5, base_y + 5), a)
                )
                cls.test_on_boundary_point_set.append(
                    cls.wrap(cls.make_point(base_x + 5, base_y + 5), b)
                )

                cls.test_outside_point_set.append(
                    cls.wrap(cls.make_point(base_x + 6, base_y + 6), a)
                )
                cls.test_outside_point_set.append(
                    cls.wrap(cls.make_point(base_x + 6, base_y + 6), b)
                )
