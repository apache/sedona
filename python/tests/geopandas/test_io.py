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

import os
import tempfile
import pytest
import shapely
import pandas as pd
import geopandas as gpd
import pyspark.pandas as ps
from functools import partial
from sedona.spark.geopandas import GeoDataFrame, GeoSeries, read_file, read_parquet
from tests import tests_resource
from tests.geopandas.test_geopandas_base import TestGeopandasBase
from shapely.geometry import (
    Point,
    Polygon,
    MultiPoint,
    MultiLineString,
    LineString,
    MultiPolygon,
    GeometryCollection,
    LinearRing,
)
from packaging.version import parse as parse_version

TEST_DATA_DIR = os.path.join("..", "spark", "common", "src", "test", "resources")


@pytest.mark.skipif(
    parse_version(shapely.__version__) < parse_version("2.0.0"),
    reason=f"Tests require shapely>=2.0.0, but found v{shapely.__version__}",
)
class TestIO(TestGeopandasBase):
    def setup_method(self):
        self.tempdir = tempfile.mkdtemp()

    #########################################################
    # File reading tests
    #########################################################

    # Modified version of Sedona's test_shapefile.py test_read_simple
    @pytest.mark.parametrize(
        "read_func",
        [
            partial(GeoDataFrame.from_file, format="shapefile"),
            partial(read_file, format="Shapefile"),
        ],
    )
    def test_read_shapefile(self, read_func):
        data_dir = os.path.join(tests_resource, "shapefiles/polygon")

        df = read_func(data_dir)

        assert df.count().item() == 10000

        subset_df = GeoDataFrame(df.head(100))
        # assert only one column
        assert subset_df.shape[1] == 1

        # assert all geometries are polygons or multipolygons
        assert subset_df["geometry"].geom_type.isin(["Polygon", "MultiPolygon"]).all()

        # Check inference and single file works
        data_file = os.path.join(data_dir, "map.shp")
        df = read_func(data_file)

        assert df.count().item() == 10000

    @pytest.mark.parametrize(
        "read_func",
        [
            partial(GeoDataFrame.from_file, format="geojson"),
            partial(read_file, format="GeoJSON"),
            partial(read_file),  # check format inference works
        ],
    )
    def test_read_geojson(self, read_func):
        datafile = os.path.join(TEST_DATA_DIR, "geojson/test1.json")
        df = read_func(datafile)
        assert (df.count() == 1).all()

        # Check that inference works
        df = read_func(datafile)
        assert (df.count() == 1).all()

    @pytest.mark.parametrize(
        "read_func",
        [
            partial(GeoDataFrame.from_file, format="geoparquet"),
            partial(read_file, format="GeoParquet"),
            partial(read_file),  # check format inference works
            read_parquet,
        ],
    )
    def test_read_geoparquet(self, read_func):
        input_location = os.path.join(TEST_DATA_DIR, "geoparquet/example1.parquet")
        df = read_func(input_location)
        # check that all column counts are 5
        assert (df.count() == 5).all()

        # Check that inference works
        df = read_func(input_location)
        assert (df.count() == 5).all()

    # From Sedona's GeoPackageReaderTest.scala
    @pytest.mark.parametrize(
        "read_func",
        [
            partial(GeoDataFrame.from_file, format="geopackage"),
            partial(read_file, format="GeoPackage"),
            partial(read_file),  # check format inference works
        ],
    )
    def test_read_geopackage(self, read_func):
        datafile = os.path.join(TEST_DATA_DIR, "geopackage/features.gpkg")

        table_name = "GB_Hex_5km_GS_CompressibleGround_v8"
        expected_cnt = 4233
        df = read_func(datafile, table_name=table_name)
        assert df["geom"].count() == expected_cnt

        # Ensure inference works
        table_name = "GB_Hex_5km_GS_Landslides_v8"
        expected_cnt = 4228
        df = read_func(datafile, table_name=table_name)
        assert df["geom"].count() == expected_cnt

    #########################################################
    # File writing tests
    #########################################################

    def _get_next_temp_file_path(self, ext: str):
        temp_file_path = os.path.join(
            self.tempdir, next(tempfile._get_candidate_names()) + "." + ext
        )
        return temp_file_path

    @pytest.mark.parametrize(
        "write_func",
        [
            partial(GeoDataFrame.to_file, driver="GeoParquet"),
            partial(GeoDataFrame.to_file, driver="geoparquet"),
            partial(GeoDataFrame.to_file),  # check format inference works
            GeoDataFrame.to_parquet,
        ],
    )
    def test_to_geoparquet(self, write_func):
        sgpd_df = GeoDataFrame(
            {"geometry": [Point(0, 0), LineString([(0, 0), (1, 1)])], "int": [1, 2]}
        )

        temp_file_path = self._get_next_temp_file_path("parquet")

        self._apply_func(sgpd_df, write_func, temp_file_path)

        # Ensure reading from geopandas creates the same resulting GeoDataFrame
        gpd_df = gpd.read_parquet(temp_file_path)
        self.check_sgpd_df_equals_gpd_df(sgpd_df, gpd_df)

    @pytest.mark.parametrize(
        "write_func",
        [
            partial(GeoDataFrame.to_file, driver="geojson"),  # index=None here is False
            partial(GeoDataFrame.to_file, driver="GeoJSON", index=True),
            partial(GeoDataFrame.to_file, driver="geojson", index=True),
            partial(GeoDataFrame.to_file),  # check format inference works
        ],
    )
    def test_to_geojson(self, write_func):
        sgpd_df = GeoDataFrame(
            {"geometry": [Point(0, 0), LineString([(0, 0), (1, 1)])], "int": [1, 2]},
            index=[1, 2],
        )
        temp_file_path = self._get_next_temp_file_path("json")
        self._apply_func(sgpd_df, write_func, temp_file_path)

        read_result = GeoDataFrame.from_file(
            temp_file_path, format="geojson"
        ).to_geopandas()

        # if index was true, the contents should be in the same order as the original GeoDataFrame
        if write_func.keywords.get("index", None) == True:
            self.check_sgpd_df_equals_gpd_df(sgpd_df, read_result)
        else:
            # if index was not kept, just check we have all rows and we have default index
            self.check_index_equal(read_result, pd.Index([0, 1]))

    @pytest.mark.parametrize(
        "write_func",
        [
            partial(GeoDataFrame.to_file, driver="geojson"),
        ],
    )
    def test_to_file_non_int_index(self, write_func):
        sgpd_df = GeoDataFrame(
            {"geometry": [Point(0, 0), LineString([(0, 0), (1, 1)])], "int": [1, 2]},
            index=["a", "b"],
        )
        temp_file_path = self._get_next_temp_file_path("json")
        self._apply_func(sgpd_df, write_func, temp_file_path)

        read_result = GeoDataFrame.from_file(
            temp_file_path, format="geojson"
        ).to_geopandas()

        # Since index was of non-int dtype, index=None here is True
        self.check_sgpd_df_equals_gpd_df(sgpd_df, read_result)

    @pytest.mark.parametrize(
        "format",
        [
            "geojson",
            "geoparquet",
        ],
    )
    def test_to_file_and_from_file_series(self, format):
        sgpd_ser = GeoSeries([Point(0, 0), LineString([(0, 0), (1, 1)])])
        ext = format.replace("geo", "")
        temp_file_path = self._get_next_temp_file_path(ext)

        sgpd_ser.to_file(temp_file_path, driver=format, index=True)

        read_result = GeoSeries.from_file(temp_file_path, format=format)
        read_result = read_result.to_geopandas()

        # In Geopandas, the name of the series is always read in to be "geometry"
        sgpd_ser.name = "geometry"

        # Since index=True, the contents should be in the same order as the original GeoSeries
        self.check_sgpd_equals_gpd(sgpd_ser, read_result)

    def _apply_func(self, obj, func, *args):
        """
        Helper function to conditionally apply functions or methods to an object correctly.
        """
        if type(func) == str:
            return getattr(obj, func)(*args)
        else:
            return func(obj, *args)
