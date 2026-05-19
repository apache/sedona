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
    LineString,
    Polygon,
    GeometryCollection,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    LinearRing,
    box,
)
import shapely

from sedona.spark.geopandas import GeoDataFrame, GeoSeries
from tests.geopandas.test_geopandas_base import TestGeopandasBase
import pyspark.pandas as ps
import pandas as pd
import geopandas as gpd
import sedona.spark.geopandas as sgpd
import pytest
from pandas.testing import assert_frame_equal
from packaging.version import parse as parse_version


@pytest.mark.skipif(
    parse_version(shapely.__version__) < parse_version("2.0.0"),
    reason=f"Tests require shapely>=2.0.0, but found v{shapely.__version__}",
)
class TestGeoDataFrame(TestGeopandasBase):
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

    def test_construct_from_geopandas(self):
        gpd_df = gpd.GeoDataFrame(
            {"geometry1": [Point(0, 0)]}, geometry="geometry1", crs="EPSG:3857"
        )
        with ps.option_context("compute.ops_on_diff_frames", True):
            sgpd_df = GeoDataFrame(gpd_df)
        assert sgpd_df.crs == "EPSG:3857"
        assert sgpd_df.geometry.crs == "EPSG:3857"
        assert sgpd_df.geometry.name == "geometry1"

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
        expected = pd.Series([1.0, 1.0, 1.0])
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

    def test_to_geopandas(self):
        from geopandas.testing import assert_geodataframe_equal

        data = {"geometry": [Point(x, x) for x in range(3)], "id": [1, 2, 3]}
        index = [1, 2, 3]
        crs = "EPSG:3857"
        # TODO: try to optimize this away
        with ps.option_context("compute.ops_on_diff_frames", True):
            result = GeoDataFrame(data, index=index, crs=crs).to_geopandas()
        gpd_df = gpd.GeoDataFrame(data, index=index, crs=crs)
        assert_geodataframe_equal(result, gpd_df)

    def test_to_spark_pandas(self):
        data = {"geometry": [Point(x, x) for x in range(3)], "id": [1, 2, 3]}
        index = [1, 2, 3]
        result = GeoDataFrame(data, index=index).to_spark_pandas()
        ps_df = ps.DataFrame(data, index=index)
        assert_frame_equal(result.to_pandas(), ps_df.to_pandas())

    def test_getitem(self):
        geoms = [Point(x, x) for x in range(3)]
        ids = [1, 2, 3]
        values = ["a", "b", "c"]
        crs = "EPSG:3857"

        with ps.option_context("compute.ops_on_diff_frames", True):
            df = GeoDataFrame({"geometry": geoms, "id": ids, "value": values}, crs=crs)

        # get a single non-geometry series
        result = df["id"]
        expected = pd.Series(ids, name="id")
        self.check_pd_series_equal(result, expected)

        # get a single geometry series
        result = df["geometry"]
        expected = gpd.GeoSeries(geoms, name="geometry", crs=crs)
        self.check_sgpd_equals_gpd(result, expected)

        # get multiple columns
        result = df[["id", "value"]]
        # no crs because no geometry column
        expected = gpd.GeoDataFrame({"id": ids, "value": values})
        self.check_sgpd_df_equals_gpd_df(result, expected)

        # get numerical slice
        result = df[:2]
        expected = gpd.GeoDataFrame(
            {"geometry": geoms[:2], "id": ids[:2], "value": values[:2]}, crs=crs
        )
        self.check_sgpd_df_equals_gpd_df(result, expected)

    def test_plot(self):
        # Just make sure it doesn't error
        df = GeoDataFrame(
            {
                "value1": ["a", "b", "c"],
                "geometry": [
                    Point(0, 0),
                    Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                    LineString([(0, 0), (1, 1)]),
                ],
                "value2": [1, 2, 3],
            }
        )
        df.plot()

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
        df = GeoDataFrame(
            {"test": [Point(x, x) for x in range(3)]}, index=[1, 2, 3], geometry="test"
        )
        result = df.copy()
        self.check_sgpd_df_equals_gpd_df(result, df.to_geopandas())
        self.check_sgpd_df_equals_gpd_df(df, result.to_geopandas())

    def test_set_crs(self):
        sgpd_df = sgpd.GeoDataFrame({"geometry": [Point(0, 0), Point(1, 1)]})
        with ps.option_context("compute.ops_on_diff_frames", True):
            sgpd_df.crs = 4326
        assert sgpd_df.crs.to_epsg() == 4326

        with ps.option_context("compute.ops_on_diff_frames", True):
            sgpd_df.set_crs(3857, inplace=True, allow_override=True)
        assert sgpd_df.crs.to_epsg() == 3857

        with ps.option_context("compute.ops_on_diff_frames", True):
            sgpd_df = sgpd_df.set_crs(None, allow_override=True)
        assert isinstance(sgpd_df, GeoDataFrame)
        assert sgpd_df.crs is None

        with ps.option_context("compute.ops_on_diff_frames", True):
            result = sgpd_df.set_crs(4326, allow_override=True)
        assert result.crs.to_epsg() == 4326
        assert isinstance(result, GeoDataFrame)

        # Ensure set_crs without inplace modifies a copy and not current df
        assert sgpd_df.crs is None

    def test_to_crs(self):
        from pyproj import CRS

        with ps.option_context("compute.ops_on_diff_frames", True):
            gdf = sgpd.GeoDataFrame(
                {"geometry": [Point(1, 1), Point(2, 2), Point(3, 3)]}, crs=4326
            )
        assert isinstance(gdf.crs, CRS) and gdf.crs.to_epsg() == 4326

        with ps.option_context("compute.ops_on_diff_frames", True):
            result = gdf.to_crs(3857)
        assert isinstance(result.crs, CRS) and result.crs.to_epsg() == 3857
        # Ensure original df is not modified
        assert gdf.crs.to_epsg() == 4326

        expected = gpd.GeoSeries(
            [
                Point(111319.49079327356, 111325.14286638486),
                Point(222638.98158654712, 222684.20850554455),
                Point(333958.4723798207, 334111.1714019597),
            ],
            name="geometry",
            crs=3857,
        )
        self.check_sgpd_equals_gpd(result.geometry, expected)

    def test_set_geometry(self):
        from sedona.spark.geopandas.geodataframe import MissingGeometryColumnError

        points1 = [Point(x, x) for x in range(3)]
        points2 = [Point(x + 5, x + 5) for x in range(3)]

        data = {"geometry1": points1, "geometry2": points2, "attribute": [1, 2, 3]}
        sgpd_df = sgpd.GeoDataFrame(data)

        # No geometry column set yet
        with pytest.raises(MissingGeometryColumnError):
            _ = sgpd_df.geometry

        sgpd_df.set_geometry("geometry1", inplace=True)

        assert sgpd_df.geometry.name == "geometry1"

        result = sgpd_df.set_geometry("geometry2")
        assert result.geometry.name == "geometry2"

        # Ensure original df is not modified
        assert sgpd_df.geometry.name == "geometry1"

        # Test the actual values of the geometry column equal for an area calculation
        self.check_pd_series_equal(result.area, sgpd_df["geometry2"].area.to_pandas())

        # unknown column
        with pytest.raises(ValueError):
            sgpd_df.set_geometry("nonexistent-column")

        geom = GeoSeries(
            [Point(x, y) for x, y in zip(range(5), range(5))], name="geometry2"
        )

        # new crs - setting should default to GeoSeries' crs
        gs = GeoSeries(geom, crs="epsg:3857")

        with ps.option_context("compute.ops_on_diff_frames", True):
            new_df = sgpd_df.set_geometry(gs)

        assert new_df.crs == "epsg:3857"

        # explicit crs overrides self and dataframe
        with ps.option_context("compute.ops_on_diff_frames", True):
            new_df = sgpd_df.set_geometry(gs, crs="epsg:26909")

        assert new_df.crs == "epsg:26909"
        assert new_df.geometry.crs == "epsg:26909"

        # Series should use dataframe's crs
        with ps.option_context("compute.ops_on_diff_frames", True):
            new_df = sgpd_df.set_geometry(geom.values)

        assert new_df.crs == sgpd_df.crs
        assert new_df.geometry.crs == sgpd_df.crs

    def test_set_geometry_crs(self):
        df = GeoDataFrame({"geometry1": [Point(0, 0)]})
        with ps.option_context("compute.ops_on_diff_frames", True):
            df.set_geometry("geometry1", crs="EPSG:3857", inplace=True)
        assert df.crs == "EPSG:3857"
        assert df.geometry.crs == "EPSG:3857"

        with ps.option_context("compute.ops_on_diff_frames", True):
            df = GeoDataFrame(
                {"geometry1": [Point(0, 0)]}, geometry="geometry1", crs="EPSG:3857"
            )

        assert df.crs == "EPSG:3857"
        assert df.geometry.crs == "EPSG:3857"

    def test_active_geometry_name(self):
        if parse_version(gpd.__version__) < parse_version("1.0.0"):
            return

        points1 = [Point(x, x) for x in range(3)]
        points2 = [Point(x + 5, x + 5) for x in range(3)]

        data = {"geometry1": points1, "geometry2": points2, "attribute": [1, 2, 3]}
        df = GeoDataFrame(data)

        df = df.set_geometry("geometry1")
        assert df.geometry.name == df.active_geometry_name == "geometry1"

        df.set_geometry("geometry2", inplace=True)
        assert df.geometry.name == df.active_geometry_name == "geometry2"

    def test_rename_geometry(self):
        points1 = [Point(x, x) for x in range(3)]
        points2 = [Point(x + 5, x + 5) for x in range(3)]

        data = {"geometry1": points1, "geometry2": points2, "attribute": [1, 2, 3]}
        df = GeoDataFrame(data)

        df = df.set_geometry("geometry1")
        assert df.geometry.name == "geometry1"

        df = df.rename_geometry("geometry3")
        assert df.geometry.name == "geometry3"

        # test inplace rename
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
        point = Point(0, 0)
        square = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])

        data = {"geometry": [point, square], "id": [1, 2], "value": ["a", "b"]}
        df = GeoDataFrame(data)

        result = df.buffer(0.5)

        assert type(result) is GeoSeries
        # Calculate areas to verify buffer was applied correctly
        # Point buffer with radius 0.5 should have area approximately π * 0.5² ≈ 0.785
        # Square buffer with radius 0.5 should expand the 1x1 square to 2x2 square with rounded corners

        # Check that square buffer area is greater than original (1.0)
        assert result.area[1] > 1.0

    def test_to_parquet(self):
        pass

    def test_from_arrow(self):
        if parse_version(gpd.__version__) < parse_version("1.0.0"):
            return

        import pyarrow as pa

        table = pa.table({"a": [0, 1, 2], "b": [0.1, 0.2, 0.3]})
        with pytest.raises(ValueError, match="No geometry column found"):
            GeoDataFrame.from_arrow(table)

        gdf = gpd.GeoDataFrame(
            {
                "col": [1, 2, 3, 4],
                "geometry": [
                    LineString([(0, 0), (1, 1)]),
                    box(0, 0, 10, 10),
                    Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                    Point(1, 1),
                ],
            }
        )

        result = GeoDataFrame.from_arrow(gdf.to_arrow())
        self.check_sgpd_df_equals_gpd_df(result, gdf)

        gdf = gpd.GeoDataFrame(
            {
                "col": ["a", "b", "c", "d"],
                "geometry": [
                    Point(1, 1),
                    Polygon(),
                    LineString([(0, 0), (1, 1)]),
                    None,
                ],
            }
        )

        result = GeoDataFrame.from_arrow(gdf.to_arrow())

        self.check_sgpd_df_equals_gpd_df(result, gdf)

    def test_to_json(self):
        import json

        d = {"col1": ["name1", "name2"], "geometry": [Point(1, 2), Point(2, 1)]}

        # Currently, adding the crs information later requires us to join across partitions
        with ps.option_context("compute.ops_on_diff_frames", True):
            gdf = GeoDataFrame(d, crs="EPSG:3857")

        result = gdf.to_json()

        obj = json.loads(result)
        assert obj["type"] == "FeatureCollection"
        assert obj["features"][0]["geometry"]["type"] == "Point"
        assert obj["features"][0]["geometry"]["coordinates"] == [1.0, 2.0]
        assert obj["features"][1]["geometry"]["type"] == "Point"
        assert obj["features"][1]["geometry"]["coordinates"] == [2.0, 1.0]
        assert obj["crs"]["type"] == "name"
        assert obj["crs"]["properties"]["name"] == "urn:ogc:def:crs:EPSG::3857"

        expected = (
            '{"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", \
"properties": {"col1": "name1"}, "geometry": {"type": "Point", "coordinates": [1.0,\
 2.0]}}, {"id": "1", "type": "Feature", "properties": {"col1": "name2"}, "geometry"\
: {"type": "Point", "coordinates": [2.0, 1.0]}}], "crs": {"type": "name", "properti\
es": {"name": "urn:ogc:def:crs:EPSG::3857"}}}'
        )
        assert result == expected, f"Expected {expected}, but got {result}"

    def test_to_arrow(self):
        if parse_version(gpd.__version__) < parse_version("1.0.0"):
            return

        import pyarrow as pa
        from geopandas.testing import assert_geodataframe_equal

        data = {"col1": ["name1", "name2"], "geometry": [Point(1, 2), Point(2, 1)]}

        # Ensure index is not preserved for index=False
        sgpd_df = GeoDataFrame(data, index=pd.Index([1, 2]))
        result = pa.table(sgpd_df.to_arrow(index=False))

        expected = gpd.GeoDataFrame(data)

        # Ensure we can read it from using geopandas
        gpd_df = gpd.GeoDataFrame.from_arrow(result)
        assert_geodataframe_equal(gpd_df, expected)

        # Ensure we can read it using sedona geopandas
        sgpd_df = GeoDataFrame.from_arrow(result)
        self.check_sgpd_df_equals_gpd_df(sgpd_df, expected)

        # Ensure index is preserved for index=True
        sgpd_df = GeoDataFrame(data, index=pd.Index([1, 2]))
        result = pa.table(sgpd_df.to_arrow(index=True))

        expected = gpd.GeoDataFrame(data, pd.Index([1, 2]))

        gpd_df = gpd.GeoDataFrame.from_arrow(result)
        assert_geodataframe_equal(gpd_df, expected)


# -----------------------------------------------------------------------------
# # Utils
# -----------------------------------------------------------------------------


def check_geodataframe(df):
    assert isinstance(df, GeoDataFrame)
