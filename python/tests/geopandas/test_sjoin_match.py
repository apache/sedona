import pytest
import geopandas as gpd
from shapely.geometry import Point
from sedona.geopandas import sjoin
from geopandas.tools import sjoin as gpd_sjoin
from geopandas import GeoDataFrame

class TestSJoinDWithinMatch:
    def setup_method(self):
        # Create test GeoDataFrames
        self.gdf1 = gpd.GeoDataFrame(
            {"id": [1, 2, 3]},
            geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
            crs="EPSG:4326"
        )
        self.gdf2 = gpd.GeoDataFrame(
            {"id": ["a", "b", "c"]},
            geometry=[Point(0.1, 0.1), Point(1.5, 1.5), Point(10, 10)],
            crs="EPSG:4326"
        )

    def test_dwithin_equivalence(self):
        """Ensure Sedona and GeoPandas produce same results for dwithin() join."""
        distance = 0.3

        # Sedona join
        sedona_result = sjoin(self.gdf1, self.gdf2, predicate="dwithin", distance=distance)
        # GeoPandas join
        gpd_result = gpd_sjoin(self.gdf1, self.gdf2, predicate="dwithin", distance=distance)

        assert isinstance(sedona_result, GeoDataFrame)
        assert isinstance(gpd_result, GeoDataFrame)

        # Sort and compare IDs
        sedona_pairs = set(zip(sedona_result["id_left"], sedona_result["id_right"]))
        gpd_pairs = set(zip(gpd_result["id_left"], gpd_result["id_right"]))
        assert sedona_pairs == gpd_pairs

    def test_dwithin_small_distance(self):
        """Ensure small distance returns fewer or no matches."""
        small_distance = 0.01
        sedona_small = sjoin(self.gdf1, self.gdf2, predicate="dwithin", distance=small_distance)
        assert len(sedona_small) <= 1
