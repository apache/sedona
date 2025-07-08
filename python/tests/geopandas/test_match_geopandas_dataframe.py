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

from sedona.geopandas import GeoDataFrame, GeoSeries
import geopandas as gpd
from tests.geopandas.test_geopandas_base import TestGeopandasBase
import pyspark.pandas as ps


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
        assert sgpd_df.active_geometry_name == gpd_df.active_geometry_name

        self.check_sgpd_df_equals_gpd_df(sgpd_df, gpd_df)
