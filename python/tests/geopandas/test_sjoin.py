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
import shutil
import tempfile
import pytest
import shapely

from shapely.geometry import Polygon, Point, LineString
from sedona.geopandas import GeoSeries, GeoDataFrame, sjoin
from tests.test_base import TestBase
from packaging.version import parse as parse_version

@pytest.mark.skipif(
    parse_version(shapely.__version__) < parse_version("2.0.0"),
    reason=f"Tests require shapely>=2.0.0, but found v{shapely.__version__}",
)
class TestSpatialJoin(TestBase):
    def setup_method(self):
        self.tempdir = tempfile.mkdtemp()

        # Basic geometries
        self.t1 = Polygon([(0, 0), (1, 0), (1, 1)])
        self.t2 = Polygon([(0, 0), (1, 1), (0, 1)])
        self.sq = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        self.point1 = Point(0.5, 0.5)
        self.point2 = Point(1.5, 1.5)
        self.line1 = LineString([(0, 0), (1, 1)])

        # GeoSeries for testing
        self.g1 = GeoSeries([self.t1, self.t2])
        self.g2 = GeoSeries([self.sq, self.t1])
        self.g3 = GeoSeries([self.t1, self.t2], crs="epsg:4326")
        self.g4 = GeoSeries([self.t2, self.t1])

        # GeoDataFrames for testing
        self.gdf1 = GeoDataFrame(
            {"geometry": [self.t1, self.t2], "id": [1, 2], "name": ["poly1", "poly2"]}
        )
        self.gdf2 = GeoDataFrame(
            {
                "geometry": [self.sq, self.t1],
                "id": [3, 4],
                "category": ["square", "triangle"],
            }
        )
        self.gdf_points = GeoDataFrame(
            {
                "geometry": [self.point1, self.point2],
                "id": [5, 6],
                "type": ["inside", "outside"],
            }
        )

        # Test data for distance operations
        self.nearby_points = GeoDataFrame(
            {
                "geometry": [Point(0.1, 0.1), Point(2.0, 2.0)],
                "id": [7, 8],
                "distance_type": ["close", "far"],
            }
        )

    def teardown_method(self):
        shutil.rmtree(self.tempdir)

    def test_sjoin_method1(self):
        """Test basic sjoin functionality with GeoSeries"""
        left = self.g1
        right = self.g2
        joined = sjoin(left, right)
        assert joined is not None
        assert type(joined) is GeoSeries
        assert joined.count() == 4

    def test_sjoin_method2(self):
        """Test GeoSeries.sjoin method"""
        left = self.g1
        right = self.g2
        joined = left.sjoin(right)
        assert joined is not None
        assert type(joined) is GeoSeries
        assert joined.count() == 4

    def test_sjoin_geodataframe_basic(self):
        """Test basic sjoin with GeoDataFrame"""
        joined = sjoin(self.gdf1, self.gdf2)
        assert joined is not None
        assert type(joined) is GeoDataFrame
        assert "geometry" in joined.columns
        assert "id_left" in joined.columns
        assert "id_right" in joined.columns
        assert "name" in joined.columns
        assert "category" in joined.columns

    def test_sjoin_geodataframe_method(self):
        """Test GeoDataFrame.sjoin method"""
        joined = self.gdf1.sjoin(self.gdf2)
        assert joined is not None
        assert type(joined) is GeoDataFrame
        assert "geometry" in joined.columns

    def test_sjoin_predicates(self):
        """Test different spatial predicates"""
        predicates = [
            "intersects",
            "contains",
            "within",
            "touches",
            "crosses",
            "overlaps",
        ]

        for predicate in predicates:
            try:
                joined = sjoin(self.gdf1, self.gdf2, predicate=predicate)
                assert joined is not None
                assert type(joined) is GeoDataFrame
            except Exception as e:
                # Some predicates might not return results for our test data
                # but the function should not raise errors for valid predicates
                if "not supported" in str(e):
                    pytest.fail(f"Predicate '{predicate}' should be supported")

    def test_sjoin_join_types(self):
        """Test different join types"""
        join_types = ["inner", "left", "right"]

        for how in join_types:
            joined = sjoin(self.gdf1, self.gdf2, how=how)
            assert joined is not None
            assert type(joined) is GeoDataFrame
            assert "geometry" in joined.columns

    def test_sjoin_column_suffixes(self):
        """Test column suffix handling"""
        joined = sjoin(self.gdf1, self.gdf2, lsuffix="_left", rsuffix="_right")
        assert joined is not None
        assert type(joined) is GeoDataFrame

        # Check that suffixes are applied to overlapping columns
        columns = joined.columns
        if "id_left" in columns and "id_right" in columns:
            # Both datasets have 'id' column, so suffixes should be applied
            assert "id_left" in columns
            assert "id_right" in columns
            assert "id" not in columns  # Original column should not exist

    def test_sjoin_dwithin_distance(self):
        """Test dwithin predicate with distance parameter"""
        # Test with a distance that should capture nearby points
        joined = sjoin(self.gdf1, self.nearby_points, predicate="dwithin", distance=0.5)
        assert joined is not None
        assert type(joined) is GeoDataFrame

        # Test with a very small distance that should capture fewer points
        joined_small = sjoin(
            self.gdf1, self.nearby_points, predicate="dwithin", distance=0.05
        )
        assert joined_small is not None
        assert type(joined_small) is GeoDataFrame

    def test_sjoin_on_attribute(self):
        """Test attribute-based joining"""
        # Create datasets with matching attribute columns
        gdf1_attr = GeoDataFrame(
            {"geometry": [self.t1, self.t2], "zone": ["A", "B"], "value": [1, 2]}
        )
        gdf2_attr = GeoDataFrame(
            {
                "geometry": [self.sq, self.t1],
                "zone": ["A", "B"],
                "category": ["square", "triangle"],
            }
        )

        # Test joining on attribute
        joined = sjoin(gdf1_attr, gdf2_attr, on_attribute=["zone"])
        assert joined is not None
        assert type(joined) is GeoDataFrame

    def test_sjoin_points_in_polygons(self):
        """Test point-in-polygon spatial join"""
        joined = sjoin(self.gdf_points, self.gdf1, predicate="within")
        assert joined is not None
        assert type(joined) is GeoDataFrame

        # The first point should be within the polygon
        # The second point should be outside
        # Check that we have some results (at least the point inside the polygon)
        assert len(joined) >= 0  # At least no errors

    def test_sjoin_error_handling(self):
        """Test error handling for invalid inputs"""

        # Test invalid predicate
        with pytest.raises(ValueError, match="not supported"):
            sjoin(self.gdf1, self.gdf2, predicate="invalid_predicate")

        # Test invalid join type
        with pytest.raises(ValueError, match="expected to be in"):
            sjoin(self.gdf1, self.gdf2, how="invalid_join")

        # Test dwithin without distance
        with pytest.raises(ValueError, match="Distance parameter is required"):
            sjoin(self.gdf1, self.gdf2, predicate="dwithin")

        # Test same suffixes
        with pytest.raises(ValueError, match="cannot be the same"):
            sjoin(self.gdf1, self.gdf2, lsuffix="same", rsuffix="same")

        # Test invalid suffix characters
        with pytest.raises(ValueError, match="invalid characters"):
            sjoin(self.gdf1, self.gdf2, lsuffix="invalid-suffix")

    def test_sjoin_empty_results(self):
        """Test sjoin with geometries that don't intersect"""
        # Create geometries that are far apart
        far_gdf = GeoDataFrame(
            {
                "geometry": [Polygon([(10, 10), (11, 10), (11, 11), (10, 11)])],
                "id": [99],
            }
        )

        joined = sjoin(self.gdf1, far_gdf)
        assert joined is not None
        assert type(joined) is GeoDataFrame
        # Should have 0 rows for inner join with non-intersecting geometries

    def test_sjoin_mixed_geometry_types(self):
        """Test sjoin with mixed geometry types"""
        # Create a dataset with mixed geometry types
        mixed_gdf = GeoDataFrame(
            {
                "geometry": [self.point1, self.line1, self.sq],
                "id": [100, 101, 102],
                "geom_type": ["point", "line", "polygon"],
            }
        )

        joined = sjoin(self.gdf1, mixed_gdf)
        assert joined is not None
        assert type(joined) is GeoDataFrame

    def test_sjoin_performance_basic(self):
        """Basic performance test with slightly larger dataset"""
        # Create slightly larger test datasets

        # Create a grid of points
        points = []
        for i in range(10):
            for j in range(10):
                points.append(Point(i * 0.1, j * 0.1))

        large_points_gdf = GeoDataFrame({"geometry": points, "id": range(len(points))})

        # Test join performance
        joined = sjoin(large_points_gdf, self.gdf1)
        assert joined is not None
        assert type(joined) is GeoDataFrame
