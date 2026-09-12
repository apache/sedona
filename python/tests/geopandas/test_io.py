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
import sqlite3
import tempfile
import pytest
import shapely
import pandas as pd
import geopandas as gpd
import pyspark.pandas as ps
import sedona.spark.geopandas as sgpd
from functools import partial
from sedona.spark.geopandas import GeoDataFrame, GeoSeries, read_file, read_parquet
from sedona.spark.geopandas._crs import read_crs_metadata
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


@pytest.fixture
def layer_catalog(tmp_path):
    """An empty vector layer, attributes and tiles, with no readable geometries."""
    path = tmp_path / "layers.gpkg"
    with sqlite3.connect(path) as conn:
        conn.executescript("""
            CREATE TABLE gpkg_contents (
                table_name TEXT, data_type TEXT, identifier TEXT, description TEXT,
                last_change DATETIME DEFAULT '2026-01-01T00:00:00Z',
                min_x DOUBLE, min_y DOUBLE,
                max_x DOUBLE, max_y DOUBLE, srs_id INTEGER);
            CREATE TABLE gpkg_geometry_columns (
                table_name TEXT, column_name TEXT, geometry_type_name TEXT,
                srs_id INTEGER, z INTEGER, m INTEGER);
            CREATE TABLE "empty ' points" (fid INTEGER, geom BLOB);
            CREATE TABLE notes (text TEXT);
            CREATE TABLE imagery (tile_data BLOB);
            CREATE TABLE unregistered (text TEXT);
            INSERT INTO gpkg_contents (table_name, data_type) VALUES
                ('notes', 'attributes'), ('imagery', 'tiles'),
                ('empty '' points', 'features');
            INSERT INTO gpkg_geometry_columns VALUES
                ('empty '' points', 'geom', 'POINT', 0, 0, 0);
            """)
    return path


class TestListLayers(TestGeopandasBase):
    @pytest.mark.parametrize("z", [0, 1, 2])
    @pytest.mark.parametrize("m", [0, 1, 2])
    def test_declared_core_types_and_dimensions(self, layer_catalog, z, m):
        self.spark
        cases = [
            ("GEOMETRY", "Unknown", "Unknown"),
            ("POINT", "Point", "Point Z"),
            ("LINESTRING", "LineString", "LineString Z"),
            ("POLYGON", "Polygon", "Polygon Z"),
            ("MULTIPOINT", "MultiPoint", "MultiPoint Z"),
            ("MULTILINESTRING", "MultiLineString", "MultiLineString Z"),
            ("MULTIPOLYGON", "MultiPolygon", "MultiPolygon Z"),
            ("GEOMETRYCOLLECTION", "GeometryCollection", "GeometryCollection Z"),
        ]
        with sqlite3.connect(layer_catalog) as conn:
            conn.execute("DELETE FROM gpkg_contents")
            conn.execute("DELETE FROM gpkg_geometry_columns")
            for declared, _, _ in cases:
                conn.execute(f'CREATE TABLE "{declared}" (fid INTEGER, geom BLOB)')
                # Unreadable geometry bytes prove enumeration never decodes features.
                conn.execute(f"INSERT INTO \"{declared}\" VALUES (1, X'00')")
                conn.execute(
                    "INSERT INTO gpkg_contents (table_name, data_type) VALUES (?, 'features')",
                    (declared,),
                )
                conn.execute(
                    "INSERT INTO gpkg_geometry_columns VALUES (?, 'geom', ?, 0, ?, ?)",
                    (declared, declared, z, m),
                )
        expected = pd.DataFrame(
            [(name, xy if z == 0 else xyz) for name, xy, xyz in sorted(cases)],
            columns=["name", "geometry_type"],
        )
        pd.testing.assert_frame_equal(sgpd.list_layers(layer_catalog), expected)

    def test_lists_empty_layers_and_attributes_not_tiles(self, layer_catalog):
        self.spark
        expected = pd.DataFrame(
            {"name": ["empty ' points", "notes"], "geometry_type": ["Point", None]}
        )
        pd.testing.assert_frame_equal(sgpd.list_layers(layer_catalog), expected)
        pd.testing.assert_frame_equal(sgpd.list_layers(str(layer_catalog)), expected)

    def test_geometry_metadata_is_opt_in(self, layer_catalog):
        reader = self.spark.read.format("geopackage").option("showMetadata", "true")
        original = reader.load(str(layer_catalog))
        assert original.columns == [
            "table_name",
            "data_type",
            "identifier",
            "description",
            "last_change",
            "min_x",
            "min_y",
            "max_x",
            "max_y",
            "srs_id",
        ]
        assert original.count() == 3
        enriched = reader.option("includeGeometryType", "true").load(str(layer_catalog))
        assert enriched.columns == original.columns + ["geometry_type"]
        assert enriched.count() == 3
        assert enriched.schema["geometry_type"].nullable
        rows = enriched.select(
            "table_name", "geometry_type", "_metadata.file_name"
        ).collect()
        assert {r.table_name: r.geometry_type for r in rows} == {
            "empty ' points": "Point",
            "notes": None,
            "imagery": None,
        }
        assert all(r.file_name == "layers.gpkg" for r in rows)

    @pytest.mark.parametrize("data_type", ["attributes", "tiles", None])
    def test_catalog_without_geometry_columns(self, layer_catalog, data_type):
        self.spark
        with sqlite3.connect(layer_catalog) as conn:
            conn.execute("DELETE FROM gpkg_contents")
            conn.execute("DROP TABLE gpkg_geometry_columns")
            if data_type:
                conn.execute(
                    "INSERT INTO gpkg_contents (table_name, data_type) VALUES ('notes', ?)",
                    (data_type,),
                )
        expected = pd.DataFrame(
            [("notes", None)] if data_type == "attributes" else [],
            columns=["name", "geometry_type"],
        )
        pd.testing.assert_frame_equal(sgpd.list_layers(layer_catalog), expected)

    @pytest.mark.parametrize(
        "change",
        [
            "DROP TABLE gpkg_geometry_columns",
            "DELETE FROM gpkg_geometry_columns",
            "UPDATE gpkg_geometry_columns SET z = 3",
            "UPDATE gpkg_geometry_columns SET m = -1",
            "UPDATE gpkg_geometry_columns SET z = NULL",
            "UPDATE gpkg_geometry_columns SET geometry_type_name = NULL",
            "UPDATE gpkg_geometry_columns SET geometry_type_name = 'INVALID'",
            "INSERT INTO gpkg_geometry_columns SELECT * FROM gpkg_geometry_columns",
        ],
    )
    def test_invalid_feature_metadata_fails(self, layer_catalog, change):
        self.spark
        with sqlite3.connect(layer_catalog) as conn:
            conn.execute(change)
        with pytest.raises(Exception, match="Invalid GeoPackage feature metadata"):
            sgpd.list_layers(layer_catalog)

    def test_rejects_multiple_files(self, layer_catalog):
        import shutil

        self.spark
        shutil.copyfile(layer_catalog, layer_catalog.with_name("second.gpkg"))
        with pytest.raises(Exception, match="exactly one GeoPackage file"):
            sgpd.list_layers(str(layer_catalog.parent / "*.gpkg"))

    def test_geometry_type_option_requires_metadata(self, layer_catalog):
        with pytest.raises(
            Exception, match="includeGeometryType requires showMetadata"
        ):
            self.spark.read.format("geopackage").option("tableName", "notes").option(
                "includeGeometryType", "true"
            ).load(str(layer_catalog))

    @pytest.mark.parametrize("filename", [b"file.gpkg", None, 1, ["file.gpkg"]])
    def test_rejects_non_path_inputs(self, filename):
        with pytest.raises(TypeError, match="string or path-like"):
            sgpd.list_layers(filename)

    def test_rejects_other_formats(self):
        with pytest.raises(ValueError, match="GeoPackage"):
            sgpd.list_layers("data.geojson")

    def test_missing_file_fails(self, tmp_path):
        self.spark
        with pytest.raises(Exception, match="PATH_NOT_FOUND|does not exist"):
            sgpd.list_layers(tmp_path / "missing.gpkg")

    def test_invalid_file_fails(self, tmp_path):
        self.spark
        path = tmp_path / "invalid.gpkg"
        path.write_bytes(b"not a SQLite database")
        with pytest.raises(Exception, match="not a database"):
            sgpd.list_layers(path)


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
        assert df.active_geometry_name == "geom"
        assert df["geom"].count() == expected_cnt

        # Ensure inference works
        table_name = "GB_Hex_5km_GS_Landslides_v8"
        expected_cnt = 4228
        df = read_func(datafile, table_name=table_name)
        assert df.active_geometry_name == "geom"
        assert df["geom"].count() == expected_cnt

    def test_geoseries_from_geopackage_uses_source_geometry_name(self):
        datafile = os.path.join(TEST_DATA_DIR, "geopackage/features.gpkg")

        result = GeoSeries.from_file(
            datafile,
            format="geopackage",
            table_name="GB_Hex_5km_GS_CompressibleGround_v8",
        )

        assert result.name == "geom"
        assert result.count() == 4233

    def test_file_constructors_do_not_eagerly_infer_unknown_crs(self, monkeypatch):
        source = GeoDataFrame(
            self.spark.range(1).selectExpr(
                "ST_SetSRID(ST_Point(0D, 0D), 4326) AS geometry"
            )
        )
        assert read_crs_metadata(source.geometry._internal.data_fields[0])[0] is False
        monkeypatch.setattr(sgpd.io, "read_file", lambda *args, **kwargs: source)

        constructors = (GeoDataFrame.from_file, GeoSeries.from_file)
        for position, constructor in enumerate(constructors):
            job_group = (
                "test_file_constructors_do_not_eagerly_infer_unknown_crs_" f"{position}"
            )
            self.sc.setJobGroup(job_group, "file constructor CRS handling")
            try:
                result = constructor("unused.parquet", format="geoparquet")
                job_ids = self.sc.statusTracker().getJobIdsForGroup(job_group)
            finally:
                self.sc.setJobGroup(None, None)

            assert len(job_ids) == 0
            geometry = result.geometry if isinstance(result, GeoDataFrame) else result
            assert read_crs_metadata(geometry._internal.data_fields[0])[0] is False
            assert geometry.crs.to_epsg() == 4326

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
