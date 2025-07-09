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

import numpy as np
from pyspark.sql.functions import expr
from shapely.geometry import Point, Polygon, LineString

from tests.test_base import TestBase
from sedona.geopandas import GeoSeries
from sedona.geopandas.sindex import SpatialIndex


class TestSpatialIndex(TestBase):
    """Tests for the spatial index functionality in GeoSeries."""

    def setup_method(self):
        """Set up test data."""
        # Create a GeoSeries with point geometries
        self.points = GeoSeries(
            [Point(0, 0), Point(1, 1), Point(2, 2), Point(3, 3), Point(4, 4)]
        )

        # Create a GeoSeries with polygon geometries
        self.polygons = GeoSeries(
            [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Polygon([(1, 1), (2, 1), (2, 2), (1, 2)]),
                Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),
                Polygon([(3, 3), (4, 3), (4, 4), (3, 4)]),
                Polygon([(4, 4), (5, 4), (5, 5), (4, 5)]),
            ]
        )

        # Create a GeoSeries with line geometries
        self.lines = GeoSeries(
            [
                LineString([(0, 0), (1, 1)]),
                LineString([(1, 1), (2, 2)]),
                LineString([(2, 2), (3, 3)]),
                LineString([(3, 3), (4, 4)]),
                LineString([(4, 4), (5, 5)]),
            ]
        )

    def test_sindex_property_exists(self):
        """Test that the sindex property exists on GeoSeries."""
        assert hasattr(self.points, "sindex")
        assert hasattr(self.polygons, "sindex")
        assert hasattr(self.lines, "sindex")

    def test_query_with_point(self):
        """Test querying the spatial index with a point geometry."""
        # Create a list of Shapely geometries - squares around points (0,0), (1,1), etc.
        geometries = [
            Polygon(
                [
                    (i - 0.5, j - 0.5),
                    (i + 0.5, j - 0.5),
                    (i + 0.5, j + 0.5),
                    (i - 0.5, j + 0.5),
                ]
            )
            for i, j in [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)]
        ]

        # Create a spatial index from the geometries
        geom_array = np.array(geometries, dtype=object)
        sindex = SpatialIndex(geom_array)

        # Test query with a point that should intersect with one polygon
        query_point = Point(2.2, 2.2)
        result_indices = sindex.query(query_point)
        assert len(result_indices) == 1

        # Test query with a point that intersects no polygons
        empty_point = Point(10, 10)
        empty_results = sindex.query(empty_point)
        assert len(empty_results) == 0

    def test_query_with_spark_dataframe(self):
        """Test querying the spatial index with a Spark DataFrame."""
        # Create a spatial DataFrame with polygons
        polygons_data = [
            (1, "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"),
            (2, "POLYGON((1 1, 2 1, 2 2, 1 2, 1 1))"),
            (3, "POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))"),
            (4, "POLYGON((3 3, 4 3, 4 4, 3 4, 3 3))"),
            (5, "POLYGON((4 4, 5 4, 5 5, 4 5, 4 4))"),
        ]

        df = self.spark.createDataFrame(polygons_data, ["id", "wkt"])
        spatial_df = df.withColumn("geometry", expr("ST_GeomFromWKT(wkt)"))

        # Create a SpatialIndex from the DataFrame
        sindex = SpatialIndex(spatial_df, index_type="strtree", column_name="geometry")

        # Test query with a point that should intersect with one polygon
        from shapely.geometry import Point

        query_point = Point(2.2, 2.2)

        # Execute query
        result_indices = sindex.query(query_point, "contains")

        # Verify results - should find at least one result (polygon containing the point)
        assert len(result_indices) > 0

        # Test query with a polygon that should intersect multiple polygons
        from shapely.geometry import box

        query_box = box(1.5, 1.5, 3.5, 3.5)

        # Execute query
        box_results = sindex.query(query_box, predicate="contains")

        # Verify results - should find multiple polygons
        assert len(box_results) > 1

        # Test with contains predicate
        # The query box fully contains polygon at index 2 (POLYGON((2 2, 3 2, 3 3, 2 3, 2 2)))
        contains_results = sindex.query(query_box, predicate="contains")

        # Verify contains results
        assert len(contains_results) >= 1

        # Test with a point outside any polygon
        outside_point = Point(10, 10)
        outside_results = sindex.query(outside_point)

        # Verify no results for point outside
        assert len(outside_results) == 0

    def test_nearest_method(self):
        """Test the nearest method for finding k-nearest neighbors."""

        # --------- Test with local numpy array ---------

        # Create a list of point geometries
        point_geometries = [
            Point(i, i) for i in range(5)
        ]  # Points at (0,0), (1,1), etc.

        # Create a spatial index from the geometries
        geom_array = np.array(point_geometries, dtype=object)
        local_sindex = SpatialIndex(geom_array)

        # Test finding single nearest neighbor
        query_point = Point(1.2, 1.2)
        nearest_idx = local_sindex.nearest(query_point)
        assert len(nearest_idx) == 1
        assert nearest_idx[0] == 1  # Point(1,1) should be closest

        # Test finding k=2 nearest neighbors
        nearest_2 = local_sindex.nearest(query_point, k=2)
        assert len(nearest_2) == 2
        assert set(nearest_2) == {1, 2}  # Points at (1,1) and (2,2)

        # Test with return_distance=True
        nearest_with_dist = local_sindex.nearest(query_point, k=2, return_distance=True)
        assert len(nearest_with_dist) == 2  # Returns tuple of (indices, distances)
        indices, distances = nearest_with_dist
        assert len(indices) == 2
        assert len(distances) == 2
        assert all(d >= 0 for d in distances)  # Distances should be non-negative

        # --------- Test with Spark DataFrame ---------

        # Create a spatial DataFrame with points
        points_data = [
            (1, "POINT(0 0)"),
            (2, "POINT(1 1)"),
            (3, "POINT(2 2)"),
            (4, "POINT(3 3)"),
            (5, "POINT(4 4)"),
        ]

        df = self.spark.createDataFrame(points_data, ["id", "wkt"])
        spatial_df = df.withColumn("geometry", expr("ST_GeomFromWKT(wkt)"))

        # Create a SpatialIndex from the DataFrame
        spark_sindex = SpatialIndex(
            spatial_df, index_type="strtree", column_name="geometry"
        )

        # Test finding single nearest neighbor
        query_point = Point(1.2, 1.2)
        nearest_result = spark_sindex.nearest(query_point)
        assert len(nearest_result) == 1

        # The nearest point should have id=2 (POINT(1 1))
        assert nearest_result[0].geom.wkt == "POINT (1 1)"

        # Test finding k=2 nearest neighbors
        nearest_2_results = spark_sindex.nearest(query_point, k=2)
        assert len(nearest_2_results) == 2

        # Test with return_distance=True
        nearest_with_dist = spark_sindex.nearest(query_point, k=2, return_distance=True)
        assert len(nearest_with_dist) == 2  # Returns tuple of (rows, distances)
        rows, distances = nearest_with_dist
        assert len(rows) == 2
        assert len(distances) == 2
        assert all(d >= 0 for d in distances)  # Distances should be non-negative

    def test_nearest_spark_with_various_geometries(self):
        """Test nearest with different geometry types in Spark mode."""
        # Create a spatial DataFrame with mixed geometry types
        mixed_data = [
            (1, "POINT(0 0)"),
            (2, "LINESTRING(1 1, 2 2)"),
            (3, "POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))"),
            (4, "POINT(3 3)"),
            (5, "POLYGON((4 4, 5 4, 5 5, 4 5, 4 4))"),
        ]

        df = self.spark.createDataFrame(mixed_data, ["id", "wkt"])
        spatial_df = df.withColumn("geometry", expr("ST_GeomFromWKT(wkt)"))
        spark_sindex = SpatialIndex(
            spatial_df, index_type="strtree", column_name="geometry"
        )

        # Test with point query
        query_point = Point(2.5, 2.5)
        nearest_geom = spark_sindex.nearest(query_point)

        # Should find polygon containing the point
        assert len(nearest_geom) == 1
        assert "POLYGON" in nearest_geom[0].geom.wkt

        # Test with linestring query
        query_line = LineString([(1.5, 1.5), (2.5, 2.5)])
        nearest_to_line = spark_sindex.nearest(query_line)
        assert len(nearest_to_line) == 1

    def test_nearest_spark_with_less_results_than_k(self):
        """Test nearest when less or no results are expected."""
        # Create DataFrame with only one point
        single_point = [(1, "POINT(0 0)")]
        df = self.spark.createDataFrame(single_point, ["id", "wkt"])
        spatial_df = df.withColumn("geometry", expr("ST_GeomFromWKT(wkt)"))
        spark_sindex = SpatialIndex(
            spatial_df, index_type="strtree", column_name="geometry"
        )

        # Query with k=5 when only 1 point exists
        results = spark_sindex.nearest(Point(0.1, 0.1), k=5)
        assert len(results) == 1  # Should only return the one available point

    def test_nearest_spark_with_distance_verification(self):
        """Test that nearest returns results in correct distance order."""
        # Create points in a grid pattern
        grid_points = [
            (1, "POINT(0 0)"),
            (2, "POINT(1 0)"),
            (3, "POINT(0 1)"),
            (4, "POINT(1 1)"),
            (5, "POINT(2 2)"),
        ]

        df = self.spark.createDataFrame(grid_points, ["id", "wkt"])
        spatial_df = df.withColumn("geometry", expr("ST_GeomFromWKT(wkt)"))
        spark_sindex = SpatialIndex(
            spatial_df, index_type="strtree", column_name="geometry"
        )

        # Query point at (0.5, 0.5)
        query_point = Point(0.5, 0.5)

        # Get 3 nearest with distances
        results, distances = spark_sindex.nearest(
            query_point, k=3, return_distance=True
        )

        # Verify distances are in ascending order
        assert distances[0] <= distances[1] <= distances[2]

        # Manually calculate expected distances
        expected_nearest_points = [
            Point(0, 0),
            Point(1, 0),
            Point(0, 1),
            Point(1, 1),
            Point(2, 2),
        ]
        expected_distances = [p.distance(query_point) for p in expected_nearest_points]
        expected_distances.sort()

        # Verify the first 3 distances match our calculations
        for i in range(3):
            assert abs(distances[i] - expected_distances[i]) < 0.0001

    def test_nearest_spark_with_identical_distances(self):
        """Test nearest when multiple geometries have identical distances."""
        # Create points that are equidistant from center
        equidistant_points = [
            (1, "POINT(0 1)"),  # 1 unit from center
            (2, "POINT(1 0)"),  # 1 unit from center
            (3, "POINT(0 -1)"),  # 1 unit from center
            (4, "POINT(-1 0)"),  # 1 unit from center
            (5, "POINT(2 2)"),  # Center point
        ]

        df = self.spark.createDataFrame(equidistant_points, ["id", "wkt"])
        spatial_df = df.withColumn("geometry", expr("ST_GeomFromWKT(wkt)"))
        spark_sindex = SpatialIndex(
            spatial_df, index_type="strtree", column_name="geometry"
        )

        # Query center point
        query_point = Point(0, 0)

        # Get the 4 nearest points
        results = spark_sindex.nearest(query_point, k=4)
        assert len(results) == 4

        # With return_distance, verify the center point has distance 0
        results, distances = spark_sindex.nearest(
            query_point, k=4, return_distance=True
        )
        assert min(distances) == 1.0
