import pytest

from sedona.sql.types import GeometryType
from sedona.utils.geoarrow import create_spatial_dataframe
from tests.test_base import TestBase
import geopandas as gpd


class TestGeopandasToSedonaWithArrow(TestBase):

    def test_conversion_dataframe(self):
        gdf = gpd.GeoDataFrame(
            {
                "name": ["Sedona", "Apache"],
                "geometry": gpd.points_from_xy([0, 1], [0, 1]),
            }
        )

        df = create_spatial_dataframe(self.spark, gdf)

        assert df.count() == 2
        assert df.columns == ["name", "geometry"]
        assert df.schema["geometry"].dataType == GeometryType()

    def test_different_geometry_positions(self):
        gdf = gpd.GeoDataFrame(
            {
                "geometry": gpd.points_from_xy([0, 1], [0, 1]),
                "name": ["Sedona", "Apache"],
            }
        )

        gdf2 = gpd.GeoDataFrame(
            {
                "name": ["Sedona", "Apache"],
                "name1": ["Sedona", "Apache"],
                "name2": ["Sedona", "Apache"],
                "geometry": gpd.points_from_xy([0, 1], [0, 1]),
            }
        )

        df1 = create_spatial_dataframe(self.spark, gdf)
        df2 = create_spatial_dataframe(self.spark, gdf2)

        assert df1.count() == 2
        assert df1.columns == ["geometry", "name"]
        assert df1.schema["geometry"].dataType == GeometryType()

        assert df2.count() == 2
        assert df2.columns == ["name", "name1", "name2", "geometry"]
        assert df2.schema["geometry"].dataType == GeometryType()

    def test_multiple_geometry_columns(self):
        gdf = gpd.GeoDataFrame(
            {
                "name": ["Sedona", "Apache"],
                "geometry": gpd.points_from_xy([0, 1], [0, 1]),
                "geometry2": gpd.points_from_xy([0, 1], [0, 1]),
            }
        )

        df = create_spatial_dataframe(self.spark, gdf)

        assert df.count() == 2
        assert df.columns == ["name", "geometry2", "geometry"]
        assert df.schema["geometry"].dataType == GeometryType()
        assert df.schema["geometry2"].dataType == GeometryType()

    def test_missing_geometry_column(self):
        gdf = gpd.GeoDataFrame(
            {
                "name": ["Sedona", "Apache"],
            },
        )

        with pytest.raises(ValueError):
            create_spatial_dataframe(self.spark, gdf)
