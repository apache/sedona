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

import pytest
import numpy as np
import shapely
from pyspark.sql.functions import expr
from shapely.geometry import Point, Polygon, LineString, box

from tests.test_base import TestBase
from sedona.spark.geopandas import GeoSeries
from sedona.spark.geopandas.sindex import SpatialIndex
from packaging.version import parse as parse_version


@pytest.mark.skipif(
    parse_version(shapely.__version__) < parse_version("2.0.0"),
    reason=f"Tests require shapely>=2.0.0, but found v{shapely.__version__}",
)
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

    def test_construct_from_geoseries(self):
        # Construct from a GeoSeries
        gs = GeoSeries([Point(x, x) for x in range(5)])
        sindex = SpatialIndex(gs)
        result = sindex.query(Point(2, 2))
        # SpatialIndex constructed from GeoSeries return geometries
        assert result == [Point(2, 2)]

    def test_construct_from_pyspark_dataframe(self):
        # Construct from PySparkDataFrame
        df = self.spark.createDataFrame(
            [(Point(x, x),) for x in range(5)], ["geometry"]
        )
        sindex = SpatialIndex(df, column_name="geometry")
        result = sindex.query(Point(2, 2))
        assert result == [Point(2, 2)]

    def test_construct_from_nparray(self):
        # Construct from np.array
        array = np.array([Point(x, x) for x in range(5)])
        sindex = SpatialIndex(array)
        result = sindex.query(Point(2, 2))
        # Returns indices like original geopandas
        assert result == np.array([2])

    def test_geoseries_sindex_property_exists(self):
        """Test that the sindex property exists on GeoSeries."""
        assert hasattr(self.points, "sindex")
        assert hasattr(self.polygons, "sindex")
        assert hasattr(self.lines, "sindex")

    def test_geodataframe_sindex_property_exists(self):
        """Test that the sindex property exists on GeoDataFrame."""
        assert hasattr(self.points.to_geoframe(), "sindex")
        assert hasattr(self.polygons.to_geoframe(), "sindex")
        assert hasattr(self.lines.to_geoframe(), "sindex")

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
        result_indices = sindex.query(query_point, "intersects")

        # Verify results - should find at least one result (polygon containing the point)
        assert len(result_indices) > 0

        # Test query with a polygon that should intersect multiple polygons
        from shapely.geometry import box

        query_box = box(1.5, 1.5, 3.5, 3.5)

        # Execute query
        box_results = sindex.query(query_box, predicate="contains")

        # Verify results - should find multiple polygons
        assert len(box_results) == 1

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
        assert nearest_result[0].wkt == "POINT (1 1)"

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
        assert "POLYGON" in nearest_geom[0].wkt

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

    def test_intersection_method(self):
        """Test the intersection method for finding geometries that intersect a bounding box."""

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
        spark_sindex = SpatialIndex(
            spatial_df, index_type="strtree", column_name="geometry"
        )

        # Test intersection with a bounding box that should intersect with middle polygons
        bounds = (
            1.5,
            1.5,
            3.5,
            3.5,
        )  # Should intersect with polygons at (2,2) and (3,3)
        result_rows = spark_sindex.intersection(bounds)

        # Verify correct results are returned
        expected = [
            Polygon([(1, 1), (2, 1), (2, 2), (1, 2), (1, 1)]),
            Polygon([(2, 2), (3, 2), (3, 3), (2, 3), (2, 2)]),
            Polygon([(3, 3), (4, 3), (4, 4), (3, 4), (3, 3)]),
        ]
        assert result_rows == expected

        # Test with bounds that don't intersect any geometry
        empty_bounds = (10, 10, 11, 11)
        empty_results = spark_sindex.intersection(empty_bounds)
        assert len(empty_results) == 0

        # Test with bounds that cover all geometries
        full_bounds = (-1, -1, 6, 6)
        full_results = spark_sindex.intersection(full_bounds)
        expected = [
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]),
            Polygon([(1, 1), (2, 1), (2, 2), (1, 2), (1, 1)]),
            Polygon([(2, 2), (3, 2), (3, 3), (2, 3), (2, 2)]),
            Polygon([(3, 3), (4, 3), (4, 4), (3, 4), (3, 3)]),
            Polygon([(4, 4), (5, 4), (5, 5), (4, 5), (4, 4)]),
        ]
        assert full_results == expected

    def test_intersection_with_points(self):
        """Test the intersection method with point geometries."""
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
        spark_sindex = SpatialIndex(
            spatial_df, index_type="strtree", column_name="geometry"
        )

        # Test with bounds containing points 2, 3
        bounds = (0.5, 0.5, 2.5, 2.5)
        results = spark_sindex.intersection(bounds)

        # Verify correct results
        assert len(results) == 2

    def test_intersection_with_linestrings(self):
        """Test the intersection method with linestring geometries."""
        # Create a spatial DataFrame with linestrings
        lines_data = [
            (1, "LINESTRING(0 0, 1 1)"),
            (2, "LINESTRING(1 1, 2 2)"),
            (3, "LINESTRING(2 2, 3 3)"),
            (4, "LINESTRING(3 3, 4 4)"),
            (5, "LINESTRING(4 4, 5 5)"),
        ]

        df = self.spark.createDataFrame(lines_data, ["id", "wkt"])
        spatial_df = df.withColumn("geometry", expr("ST_GeomFromWKT(wkt)"))
        spark_sindex = SpatialIndex(
            spatial_df, index_type="strtree", column_name="geometry"
        )

        # Test with bounds crossing lines 2, 3
        bounds = (1.5, 1.5, 2.5, 2.5)
        results = spark_sindex.intersection(bounds)

        # Verify results
        assert len(results) == 2

    def test_intersection_with_mixed_geometries(self):
        """Test the intersection method with mixed geometry types."""
        # Create a spatial DataFrame with mixed geometry types
        mixed_data = [
            (1, "POINT(0 0)"),
            (2, "LINESTRING(1 1, 2 2)"),
            (3, "POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))"),
            (4, "POINT(3 3)"),
            (5, "LINESTRING(4 4, 5 5)"),
        ]

        df = self.spark.createDataFrame(mixed_data, ["id", "wkt"])
        spatial_df = df.withColumn("geometry", expr("ST_GeomFromWKT(wkt)"))
        spark_sindex = SpatialIndex(
            spatial_df, index_type="strtree", column_name="geometry"
        )

        # Test with bounds that should intersect with polygon and point
        bounds = (2.5, 2.5, 3.5, 3.5)
        results = spark_sindex.intersection(bounds)

        # Verify results
        assert len(results) == 2

    # test from the geopandas docstring
    def test_geoseries_sindex_intersection(self):
        gs = GeoSeries([Point(x, x) for x in range(10)])
        result = gs.sindex.intersection(box(1, 1, 3, 3).bounds)
        # Unlike original geopandas, this returns geometries instead of indices
        expected = [Point(1, 1), Point(2, 2), Point(3, 3)]
        assert result == expected
