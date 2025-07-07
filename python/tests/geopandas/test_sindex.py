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
