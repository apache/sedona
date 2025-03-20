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

import pytest

from sedona.sql.types import GeometryType
from sedona.utils.geoarrow import create_spatial_dataframe
from tests.test_base import TestBase
import geopandas as gpd
import pyspark


class TestGeopandasToSedonaWithArrow(TestBase):

    @pytest.mark.skipif(
        not pyspark.__version__.startswith("3.5"),
        reason="It's only working with Spark 3.5",
    )
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

    @pytest.mark.skipif(
        not pyspark.__version__.startswith("3.5"),
        reason="It's only working with Spark 3.5",
    )
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

    @pytest.mark.skipif(
        not pyspark.__version__.startswith("3.5"),
        reason="It's only working with Spark 3.5",
    )
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

    @pytest.mark.skipif(
        not pyspark.__version__.startswith("3.5"),
        reason="It's only working with Spark 3.5",
    )
    def test_missing_geometry_column(self):
        gdf = gpd.GeoDataFrame(
            {
                "name": ["Sedona", "Apache"],
            },
        )

        with pytest.raises(ValueError):
            create_spatial_dataframe(self.spark, gdf)
