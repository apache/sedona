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

from shapely.geometry import (
    Point,
    Polygon,
)
import shapely

from sedona.geopandas import GeoDataFrame, GeoSeries
from tests.geopandas.test_geopandas_base import TestGeopandasBase
import pyspark.pandas as ps
import pandas as pd
import geopandas as gpd
import sedona.geopandas as sgpd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal
from packaging.version import parse as parse_version


@pytest.mark.skipif(
    parse_version(shapely.__version__) < parse_version("2.0.0"),
    reason=f"Tests require shapely>=2.0.0, but found v{shapely.__version__}",
)
class TestDataframe(TestGeopandasBase):
    @pytest.mark.parametrize(
        "obj",
        [
            [Point(x, x) for x in range(3)],
            {"geometry": [Point(x, x) for x in range(3)]},
            pd.DataFrame([Point(x, x) for x in range(3)]),
            gpd.GeoDataFrame([Point(x, x) for x in range(3)]),
            pd.Series([Point(x, x) for x in range(3)]),
            gpd.GeoSeries([Point(x, x) for x in range(3)]),
        ],
    )
    def test_constructor(self, obj):
        sgpd_df = GeoDataFrame(obj)
        check_geodataframe(sgpd_df)

    @pytest.mark.parametrize(
        "obj",
        [
            pd.DataFrame(
                {
                    "non-geom": [1, 2, 3],
                    "geometry": [
                        Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]) for _ in range(3)
                    ],
                }
            ),
            gpd.GeoDataFrame(
                {
                    "geom2": [Point(x, x) for x in range(3)],
                    "non-geom": [4, 5, 6],
                    "geometry": [
                        Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]) for _ in range(3)
                    ],
                }
            ),
        ],
    )
    def test_complex_df(self, obj):
        sgpd_df = GeoDataFrame(obj)
        name = "geometry"
        sgpd_df.set_geometry(name, inplace=True)
        check_geodataframe(sgpd_df)
        result = sgpd_df.area
        expected = pd.Series([1.0, 1.0, 1.0], name=name)
        self.check_pd_series_equal(result, expected)

    # These need to be defined inside the function to ensure Sedona's Geometry UDTs have been registered
    def test_constructor_pandas_on_spark(self):
        for obj in [
            ps.DataFrame([Point(x, x) for x in range(3)]),
            ps.Series([Point(x, x) for x in range(3)]),
            GeoSeries([Point(x, x) for x in range(3)]),
            GeoDataFrame([Point(x, x) for x in range(3)]),
        ]:
            sgpd_df = GeoDataFrame(obj)
            check_geodataframe(sgpd_df)

    @pytest.mark.parametrize(
        "obj",
        [
            [0, 1, 2],
            ["x", "y", "z"],
            {"a": [0, 1, 2], 1: [4, 5, 6]},
            {"a": ["x", "y", "z"], 1: ["a", "b", "c"]},
            pd.Series([0, 1, 2]),
            pd.Series(["x", "y", "z"]),
            pd.DataFrame({"x": ["x", "y", "z"]}),
            gpd.GeoDataFrame({"x": [0, 1, 2]}),
            ps.DataFrame({"x": ["x", "y", "z"]}),
        ],
    )
    def test_non_geometry(self, obj):
        pd_df = pd.DataFrame(obj)
        # pd.DataFrame(obj) doesn't work correctly for pandas on spark DataFrame type, so we use to_pandas() method instead.
        if isinstance(obj, ps.DataFrame):
            pd_df = obj.to_pandas()
        sgpd_df = sgpd.GeoDataFrame(obj)
        assert_frame_equal(pd_df, sgpd_df.to_pandas())

    def test_psdf(self):
        # this is to make sure the spark session works with pandas on spark api
        psdf = ps.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6],
                "b": [100, 200, 300, 400, 500, 600],
                "c": ["one", "two", "three", "four", "five", "six"],
            },
            index=[10, 20, 30, 40, 50, 60],
        )
        assert psdf.count().count() == 3

    def test_type_single_geometry_column(self):
        # Create a GeoDataFrame with a single geometry column and additional attributes
        points = [Point(x, x) for x in range(3)]
        data = {"geometry1": points, "id": [1, 2, 3], "value": ["a", "b", "c"]}

        df = GeoDataFrame(data)

        # Verify the GeoDataFrame type
        assert type(df) is GeoDataFrame

        # Check the underlying Spark DataFrame schema
        schema = df._internal.spark_frame.schema

        # Assert the geometry column has the correct type and is not nullable
        geometry_field = schema["geometry1"]
        assert (
            geometry_field.dataType.typeName() == "geometrytype"
            or geometry_field.dataType.typeName() == "binary"
        )
        assert not geometry_field.nullable

        # Assert non-geometry columns are present with correct types
        assert schema["id"].dataType.typeName().startswith("long")
        assert schema["value"].dataType.typeName().startswith("string")

        # Verify number of columns
        assert len(schema.fields) == 5

    def test_type_multiple_geometry_columns(self):
        # Create points for two geometry columns
        points1 = [Point(x, x) for x in range(3)]
        points2 = [Point(x + 5, x + 5) for x in range(3)]

        # Create a dictionary with two geometry columns
        data = {"geometry1": points1, "geometry2": points2, "attribute": [1, 2, 3]}

        df = GeoDataFrame(data)
        assert type(df) is GeoDataFrame

        schema = df._internal.spark_frame.schema
        # Assert both geometry columns have the correct type
        geometry_field1 = schema["geometry1"]
        assert (
            geometry_field1.dataType.typeName() == "geometrytype"
            or geometry_field1.dataType.typeName() == "binary"
        )
        assert not geometry_field1.nullable

        geometry_field2 = schema["geometry2"]
        assert (
            geometry_field2.dataType.typeName() == "geometrytype"
            or geometry_field2.dataType.typeName() == "binary"
        )
        assert not geometry_field2.nullable

        # Check non-geometry column
        attribute_field = schema["attribute"]
        assert (
            attribute_field.dataType.typeName() != "geometrytype"
            and attribute_field.dataType.typeName() != "binary"
        )

    def test_copy(self):
        df = GeoDataFrame([Point(x, x) for x in range(3)], name="test_df")
        df_copy = df.copy()
        assert type(df_copy) is GeoDataFrame

    def test_set_geometry(self):
        points1 = [Point(x, x) for x in range(3)]
        points2 = [Point(x + 5, x + 5) for x in range(3)]

        data = {"geometry1": points1, "geometry2": points2, "attribute": [1, 2, 3]}
        sgpd_df = sgpd.GeoDataFrame(data)

        # No geometry column set yet
        with pytest.raises(AttributeError):
            _ = sgpd_df.geometry

        # TODO: Try to optimize this with self.ps_allow_diff_frames() away
        with self.ps_allow_diff_frames():
            sgpd_df = sgpd_df.set_geometry("geometry1")

        assert sgpd_df.geometry.name == "geometry1"

        # TODO: Try to optimize this with self.ps_allow_diff_frames() away
        with self.ps_allow_diff_frames():
            sgpd_df.set_geometry("geometry2", inplace=True)
        assert sgpd_df.geometry.name == "geometry2"

        # Test the actual values of the geometry column
        assert_series_equal(
            sgpd_df.geometry.area.to_pandas(), sgpd_df["geometry2"].area.to_pandas()
        )

        # unknown column
        with pytest.raises(ValueError):
            sgpd_df.set_geometry("nonexistent-column")

        geom = GeoSeries(
            [Point(x, y) for x, y in zip(range(5), range(5))], name="geometry2"
        )

        # new crs - setting should default to GeoSeries' crs
        gs = GeoSeries(geom, crs="epsg:3857")

        with self.ps_allow_diff_frames():
            new_df = sgpd_df.set_geometry(gs)

        assert new_df.crs == "epsg:3857"

        # explicit crs overrides self and dataframe
        with self.ps_allow_diff_frames():
            new_df = sgpd_df.set_geometry(gs, crs="epsg:26909")

        assert new_df.crs == "epsg:26909"
        assert new_df.geometry.crs == "epsg:26909"

        # Series should use dataframe's crs
        with self.ps_allow_diff_frames():
            new_df = sgpd_df.set_geometry(geom.values)

        assert new_df.crs == sgpd_df.crs
        assert new_df.geometry.crs == sgpd_df.crs

    def test_active_geometry_name(self):
        if parse_version(gpd.__version__) < parse_version("1.0.0"):
            return

        points1 = [Point(x, x) for x in range(3)]
        points2 = [Point(x + 5, x + 5) for x in range(3)]

        data = {"geometry1": points1, "geometry2": points2, "attribute": [1, 2, 3]}
        df = GeoDataFrame(data)

        # TODO: Try to optimize this with self.ps_allow_diff_frames() away
        with self.ps_allow_diff_frames():
            df = df.set_geometry("geometry1")
        assert df.geometry.name == df.active_geometry_name == "geometry1"

        # TODO: Try to optimize this with self.ps_allow_diff_frames() away
        with self.ps_allow_diff_frames():
            df.set_geometry("geometry2", inplace=True)
        assert df.geometry.name == df.active_geometry_name == "geometry2"

    @pytest.mark.skip(reason="TODO: fix this")
    def test_rename_geometry(self):
        points1 = [Point(x, x) for x in range(3)]
        points2 = [Point(x + 5, x + 5) for x in range(3)]

        data = {"geometry1": points1, "geometry2": points2, "attribute": [1, 2, 3]}
        df = GeoDataFrame(data)

        # TODO: Try to optimize all of these with self.ps_allow_diff_frames() calls away
        with self.ps_allow_diff_frames():
            df = df.set_geometry("geometry1")
        assert df.geometry.name == "geometry1"

        with self.ps_allow_diff_frames():
            df = df.rename_geometry("geometry3")
        assert df.geometry.name == "geometry3"

        # test inplace rename
        with self.ps_allow_diff_frames():
            df.rename_geometry("geometry4", inplace=True)
        assert df.geometry.name == "geometry4"

    def test_area(self):
        # Create a GeoDataFrame with polygons to test area calculation
        from shapely.geometry import Polygon

        # Create polygons with known areas (1.0 and 4.0 square units)
        poly1 = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])  # 1 square unit
        poly2 = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])  # 4 square units

        data = {"geometry1": [poly1, poly2], "id": [1, 2], "value": ["a", "b"]}

        df = GeoDataFrame(data)
        df.set_geometry("geometry1", inplace=True)

        area_series = df.area

        assert type(area_series) is ps.Series

        # Check the actual area values
        area_values = area_series.to_list()
        assert len(area_series) == 2
        self.assert_almost_equal(area_values[0], 1.0)
        self.assert_almost_equal(area_values[1], 4.0)

    def test_buffer(self):
        # Create a GeoDataFrame with geometries to test buffer operation
        from shapely.geometry import Polygon, Point

        # Create input geometries
        point = Point(0, 0)
        square = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])

        data = {"geometry1": [point, square], "id": [1, 2], "value": ["a", "b"]}
        df = GeoDataFrame(data)

        # Apply buffer with distance 0.5
        buffer_df = df.buffer(0.5)

        # Verify result is a GeoDataFrame
        assert type(buffer_df) is GeoDataFrame

        # Verify the original columns are preserved
        assert "geometry1_buffered" in buffer_df.columns
        assert "id" in buffer_df.columns
        assert "value" in buffer_df.columns

        # Convert to pandas to extract individual geometries
        pandas_df = buffer_df._internal.spark_frame.select(
            "geometry1_buffered"
        ).toPandas()

        # Calculate areas to verify buffer was applied correctly
        # Point buffer with radius 0.5 should have area approximately π * 0.5² ≈ 0.785
        # Square buffer with radius 0.5 should expand the 1x1 square to 2x2 square with rounded corners
        areas = [geom.area for geom in pandas_df["geometry1_buffered"]]

        # Check that square buffer area is greater than original (1.0)
        assert areas[1] > 1.0


# -----------------------------------------------------------------------------
# # Utils
# -----------------------------------------------------------------------------


def check_geodataframe(df):
    assert isinstance(df, GeoDataFrame)
