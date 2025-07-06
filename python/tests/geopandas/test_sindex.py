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

import unittest
import numpy as np
from shapely.geometry import Point, Polygon, LineString, box
from sedona.geopandas import GeoSeries
from sedona.geopandas.sindex import SpatialIndex


class TestSpatialIndex(unittest.TestCase):
    """Tests for the spatial index functionality in GeoSeries."""

    def setUp(self):
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

    def test_spatial_index_with_shapely_array(self):
        # Create a list of Shapely geometries
        geometries = [Point(0, 0), Point(1, 1), Point(2, 2), Point(3, 3)]

        # Convert to object array to maintain references to the geometry objects
        geom_array = np.array(geometries, dtype=object)

        # Create a SpatialIndex from the numpy array
        sindex = SpatialIndex(geom_array)

        # Verify the size of the index
        assert sindex.size == 4
        assert not sindex.is_empty
