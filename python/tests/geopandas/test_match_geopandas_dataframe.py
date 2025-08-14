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

import pytest
import shutil
import tempfile
import shapely
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
from sedona.spark.geopandas import GeoDataFrame, GeoSeries
import geopandas as gpd
from tests.geopandas.test_geopandas_base import TestGeopandasBase
import pyspark.pandas as ps


@pytest.mark.skipif(
    parse_version(shapely.__version__) < parse_version("2.0.0"),
    reason=f"Tests require shapely>=2.0.0, but found v{shapely.__version__}",
)
class TestMatchGeopandasDataFrame(TestGeopandasBase):
    def setup_method(self):
        self.tempdir = tempfile.mkdtemp()

        rows = 3

        self.points = [Point(x, x + 1, x + 2) for x in range(rows)]

        self.multipoints = [
            MultiPoint([(x, x + 1), (x + 2, x + 3)]) for x in range(rows)
        ]

        self.linestrings = [
            LineString([(x, x + 1), (x + 2, x + 3)]) for x in range(rows)
        ]

        self.multilinestrings = [
            MultiLineString(
                [[[x, x + 1], [x + 2, x + 3]], [[x + 4, x + 5], [x + 6, x + 7]]]
            )
            for x in range(rows)
        ]

        self.polygons = [
            Polygon(
                [(x, 0, x + 2), (x + 1, 0, x + 3), (x + 2, 1, x + 4), (x + 3, 1, x + 5)]
            )
            for x in range(rows)
        ]

        self.multipolygons = [
            MultiPolygon(
                [
                    (
                        [(0.0, 0.0), (0.0, 1.0), (1.0, 0.0)],
                        [[(0.1, 0.1), (0.1, 0.2), (0.2, 0.1), (0.1, 0.1)]],
                    )
                ]
            )
            for x in range(rows)
        ]

        self.geomcollection = [
            GeometryCollection(
                [
                    MultiPoint([(0, 0), (1, 1)]),
                    MultiLineString([[(0, 0), (1, 1)], [(2, 2), (3, 3)]]),
                    MultiPolygon(
                        [
                            (
                                [(0.0, 0.0), (0.0, 1.0), (1.0, 0.0)],
                                [[(0.1, 0.1), (0.1, 0.2), (0.2, 0.1), (0.1, 0.1)]],
                            )
                        ]
                    ),
                ]
            )
            for x in range(rows)
        ]

        self.geometries = {
            "points": self.points,
            "multipoints": self.multipoints,
            "linestrings": self.linestrings,
            "multilinestrings": self.multilinestrings,
            "polygons": self.polygons,
            "multipolygons": self.multipolygons,
            "geomcollection": self.geomcollection,
        }

    def teardown_method(self):
        shutil.rmtree(self.tempdir)

    def test_getitem(self):
        sgpd_df = GeoDataFrame(self.geometries)
        gpd_df = gpd.GeoDataFrame(self.geometries)
        for key in self.geometries.keys():
            actual_series, expected_series = sgpd_df[key], gpd_df[key]
            if isinstance(actual_series, GeoSeries):
                # original geopandas does not guarantee a GeoSeries will be returned, so convert it here
                expected_series = gpd.GeoSeries(expected_series)
                self.check_sgpd_equals_gpd(actual_series, expected_series)
            else:
                self.check_pd_series_equal(actual_series, expected_series)  # type: ignore

        self.check_sgpd_df_equals_gpd_df(sgpd_df, gpd_df)

    def test_set_geometry(self):
        sgpd_df = GeoDataFrame(self.geometries)
        gpd_df = gpd.GeoDataFrame(self.geometries)

        sgpd_df = sgpd_df.set_geometry("points")
        gpd_df = gpd_df.set_geometry("points")
        assert sgpd_df.geometry.name == gpd_df.geometry.name

        self.check_sgpd_df_equals_gpd_df(sgpd_df, gpd_df)

    def test_active_geometry_name(self):
        if parse_version(gpd.__version__) < parse_version("1.0.0"):
            return

        sgpd_df = GeoDataFrame(self.geometries)
        gpd_df = gpd.GeoDataFrame(self.geometries)

        sgpd_df = sgpd_df.set_geometry("polygons")
        gpd_df = gpd_df.set_geometry("polygons")
        assert sgpd_df.geometry.name == gpd_df.geometry.name
        assert (
            sgpd_df.active_geometry_name
            == gpd_df.active_geometry_name
            == sgpd_df.geometry.name
        )

    def test_rename_geometry(self):
        sgpd_df = GeoDataFrame(self.geometries)
        gpd_df = gpd.GeoDataFrame(self.geometries)

        sgpd_df = sgpd_df.set_geometry("polygons")
        gpd_df = gpd_df.set_geometry("polygons")
        assert sgpd_df.geometry.name == gpd_df.geometry.name

        # test inplace
        sgpd_df.rename_geometry("random", inplace=True)
        gpd_df.rename_geometry("random", inplace=True)
        assert sgpd_df.geometry.name == gpd_df.geometry.name

        # Ensure the names are different when we rename to different names
        sgpd_df = sgpd_df.rename_geometry("name1")
        gpd_df = gpd_df.rename_geometry("name2")
        assert sgpd_df.geometry.name != gpd_df.geometry.name

    def test_to_json(self):
        tests = [
            {
                "a": [1, 2, 3],
                "b": ["4", "5", "6"],
                "geometry": [
                    Point(1, 2),
                    Point(2, 1),
                    Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                ],
            },
            {
                "a": [1, 2, 3],
                "b": ["4", "5", "6"],
                "geometry": [
                    LineString([(0, 0), (1, 1)]),
                    GeometryCollection(Point()),
                    Point(2, 1),
                ],
            },
            {
                "a": [1, 2, 3],
                "b": ["4", "5", "6"],
                "geometry": [Polygon(), Point(1, 2), None],
            },
        ]

        for data in tests:
            sgpd_result = GeoDataFrame(data).to_json()
            gpd_result = gpd.GeoDataFrame(data).to_json()
            assert sgpd_result == gpd_result

        # test different json args
        data = {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
            "geometry": [Point(1, 2), Point(2, 1), LineString([(0, 0), (1, 1)])],
        }
        tests = [
            {"na": "drop"},
            {"na": "keep"},
            {"show_bbox": True},
            {"drop_id": True},
            {"to_wgs84": True},
            {"na": "drop", "show_bbox": True, "drop_id": True, "to_wgs84": True},
        ]
        for kwargs in tests:
            # TODO: Try to optimize this 'with ps.option_context("compute.ops_on_diff_frames", True)' away
            with ps.option_context("compute.ops_on_diff_frames", True):
                sgpd_result = GeoDataFrame(data, crs="EPSG:3857").to_json(**kwargs)
            gpd_result = gpd.GeoDataFrame(data, crs="EPSG:3857").to_json(**kwargs)
            assert sgpd_result == gpd_result

    def test_from_arrow(self):
        if parse_version(gpd.__version__) < parse_version("1.0.0"):
            return

        gdf = gpd.GeoDataFrame(
            {
                "ints": [1, 2, 3, 4],
                "strings": ["a", "b", "c", "d"],
                "bools": [True, False, True, False],
                "geometry": [
                    Point(0, 1),
                    LineString([(0, 0), (1, 1)]),
                    Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                    Point(1, 1),
                ],
            }
        )

        sgpd_result = GeoDataFrame.from_arrow(gdf.to_arrow())
        gpd_result = gpd.GeoDataFrame.from_arrow(gdf.to_arrow())
        self.check_sgpd_df_equals_gpd_df(sgpd_result, gpd_result)

    def test_to_arrow(self):
        if parse_version(gpd.__version__) < parse_version("1.0.0"):
            return

        import pyarrow as pa
        import pandas as pd

        data = {
            "a": [1, 2, 3],
            "b": [4, 5, 6],
            "geometry": [Point(1, 2), Point(2, 1), LineString([(0, 0), (1, 1)])],
        }

        sgpd_result = pa.table(GeoDataFrame(data).to_arrow(index=False))
        gpd_result = pa.table(gpd.GeoDataFrame(data).to_arrow(index=False))

        assert sgpd_result.equals(gpd_result)

        sgpd_result = pa.table(
            GeoDataFrame(data, index=pd.RangeIndex(start=0, stop=3, step=1)).to_arrow(
                index=True
            )
        )
        gpd_result = pa.table(
            gpd.GeoDataFrame(
                data, index=pd.RangeIndex(start=0, stop=3, step=1)
            ).to_arrow(index=True)
        )

        assert sgpd_result.equals(gpd_result)

        # Note: Results for not specifying index=True or index=False for to_arrow is expected to be different
        # from geopandas. See the to_arrow docstring for more details.
