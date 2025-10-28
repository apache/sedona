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
import shutil
import tempfile
import pytest
import pandas as pd
import geopandas as gpd
import pyspark.pandas as ps
import pyspark
from pandas.testing import assert_series_equal
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

from sedona.spark.geopandas import GeoSeries
from tests.geopandas.test_geopandas_base import TestGeopandasBase
import pyspark.pandas as ps
from packaging.version import parse as parse_version


@pytest.mark.skipif(
    parse_version(shapely.__version__) < parse_version("2.0.0"),
    reason=f"Tests require shapely>=2.0.0, but found v{shapely.__version__}",
)
class TestMatchGeopandasSeries(TestGeopandasBase):
    def setup_method(self):
        self.tempdir = tempfile.mkdtemp()
        self.t1 = Polygon([(0, 0), (1, 0), (1, 1)])
        self.t2 = Polygon([(0, 0), (1, 1), (0, 1)])
        self.sq = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        self.g1 = GeoSeries([self.t1, self.t2])
        self.g2 = GeoSeries([self.sq, self.t1])
        self.g3 = GeoSeries([self.t1, self.t2], crs="epsg:4326")
        self.g4 = GeoSeries([self.t2, self.t1])

        self.points = [Point(), Point(0, 0), Point(1, 2)]

        self.multipoints = [
            MultiPoint(),
            MultiPoint([(0, 0), (1, 1)]),
            MultiPoint([(1, 2), (3, 4)]),
        ]

        self.linestrings = [
            LineString(),
            LineString([(0, 0), (1, 1)]),
            LineString([(1, 2), (3, 4)]),
        ]

        self.linearrings = [
            LinearRing(),
            LinearRing([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]),
            LinearRing([(1, 1), (2, 1), (2, 2), (1, 2), (1, 1)]),
        ]

        self.multilinestrings = [
            MultiLineString(),
            MultiLineString([[(0, 1), (2, 3)], [(4, 5), (6, 7)]]),
            MultiLineString([[(1, 2), (3, 4)], [(5, 6), (7, 8)]]),
        ]

        self.polygons = [
            Polygon(),
            Polygon([(0, 0), (1, 0), (2, 1), (3, 1)]),
            Polygon([(1, 1), (2, 1), (2, 2), (1, 2)]),
        ]

        self.multipolygons = [
            MultiPolygon(),
            MultiPolygon(
                [
                    (
                        [(0.0, 0.0), (0.0, 1.0), (1.0, 0.0)],
                        [[(0.1, 0.1), (0.1, 0.2), (0.2, 0.1), (0.1, 0.1)]],
                    )
                ]
            ),
        ]

        self.geomcollection = [
            GeometryCollection(),
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
            ),
        ]

        self.geoms = [
            self.points,
            self.multipoints,
            self.linestrings,
            self.linearrings,
            self.multilinestrings,
            self.polygons,
            self.multipolygons,
            self.geomcollection,
        ]

        self.pairs = [
            (self.points, self.multipolygons),
            (self.geomcollection, self.polygons),
            (self.linestrings, self.multipoints),
            (self.linearrings, self.multilinestrings),
        ]

    def teardown_method(self):
        shutil.rmtree(self.tempdir)

    def test_constructor(self):
        for geom in self.geoms:
            gpd_series = gpd.GeoSeries(geom)
            assert isinstance(gpd_series, gpd.GeoSeries)
            assert isinstance(gpd_series.geometry, gpd.GeoSeries)

    def test_to_geopandas(self):
        for geom in self.geoms:
            sgpd_result = GeoSeries(geom)
            gpd_result = gpd.GeoSeries(geom)
            # The below method calls to_geopandas() on sgpd_result, so we don't do it here
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

        # Ensure we have the same result for empty GeoSeries
        sgpd_series = GeoSeries([])
        gpd_series = gpd.GeoSeries([])
        self.check_sgpd_equals_gpd(sgpd_series, gpd_series)

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

    def test_internal_st_function(self):
        # this is to make sure the spark session works with internal sedona udfs
        baseDf = self.spark.sql(
            "SELECT ST_GeomFromWKT('POLYGON ((50 50 1, 50 80 2, 80 80 3, 80 50 2, 50 50 1))') as geom"
        )
        actual = baseDf.selectExpr("ST_AsText(ST_Expand(geom, 10))").first()[0]
        expected = "POLYGON Z((40 40 -9, 40 90 -9, 90 90 13, 90 40 13, 40 40 -9))"
        assert expected == actual

    def test_type(self):
        assert type(self.g1) is GeoSeries
        assert type(self.g2) is GeoSeries
        assert type(self.g3) is GeoSeries
        assert type(self.g4) is GeoSeries

    def test_copy(self):
        gc = self.g3.copy()
        assert type(gc) is GeoSeries
        assert self.g3.name == gc.name

    def test_area(self):
        area = self.g1.area
        assert area is not None
        assert type(area) is ps.Series
        assert area.count() == 2

        for geom in self.geoms:
            sgpd_result = GeoSeries(geom).area
            gpd_result = gpd.GeoSeries(geom).area
            self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_buffer(self):
        for geom in self.geoms:
            dist = 0.2
            sgpd_result = GeoSeries(geom).buffer(dist)
            gpd_result = gpd.GeoSeries(geom).buffer(dist)

            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

        # Check that the parameters work properly
        sgpd_result = GeoSeries(self.linestrings).buffer(
            0.2, resolution=20, cap_style="flat", join_style="bevel"
        )
        gpd_result = gpd.GeoSeries(self.linestrings).buffer(
            0.2, resolution=20, cap_style="flat", join_style="bevel"
        )
        self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

        sgpd_result = GeoSeries(self.linestrings).buffer(0.2, single_sided=True)
        gpd_result = gpd.GeoSeries(self.linestrings).buffer(0.2, single_sided=True)
        self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

        sgpd_result = GeoSeries(self.linestrings).buffer(
            0.2, join_style="mitre", mitre_limit=10.0
        )
        gpd_result = gpd.GeoSeries(self.linestrings).buffer(
            0.2, join_style="mitre", mitre_limit=10.0
        )
        self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

        sgpd_result = GeoSeries(self.linestrings).buffer(
            0.2, single_sided=True, cap_style="round"
        )
        gpd_result = gpd.GeoSeries(self.linestrings).buffer(
            0.2, single_sided=True, cap_style="round"
        )
        self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_buffer_then_area(self):
        area = self.g1.buffer(0.2).area
        assert area is not None
        assert type(area) is ps.Series
        assert area.count() == 2

    @pytest.mark.skip(
        reason="Slight differences in results make testing this difficult"
    )
    # Changing tests in anyway often make this test fail, since results often differ slightly
    # e.g. POLYGON ((1 2, 2 1, 2 2, 1 2)) and POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))
    # It's more convenient to turn this off to smoothen development to avoid having to "fine-tune" the tests
    # Note: simplify() is still tested in test_geoseries.py to ensure it's hooked up properly
    def test_simplify(self):
        for geom in self.geoms:
            if isinstance(geom[0], LinearRing):
                continue
            sgpd_result = GeoSeries(geom).simplify(100.1)
            gpd_result = gpd.GeoSeries(geom).simplify(100.1)
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

            sgpd_result = GeoSeries(geom).simplify(0.05, preserve_topology=False)
            gpd_result = gpd.GeoSeries(geom).simplify(0.05, preserve_topology=False)
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_geometry(self):
        for geom in self.geoms:
            gpd_result = gpd.GeoSeries(geom).geometry
            sgpd_result = GeoSeries(geom).geometry
            assert isinstance(sgpd_result, GeoSeries)
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_x(self):
        for pt in self.points:
            sgpd_result = GeoSeries(pt).x
            assert isinstance(sgpd_result, ps.Series)
            gpd_result = gpd.GeoSeries(pt).x
            self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_y(self):
        for pt in self.points:
            sgpd_result = GeoSeries(pt).y
            assert isinstance(sgpd_result, ps.Series)
            gpd_result = gpd.GeoSeries(pt).y
            self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_z(self):
        for pt in self.points:
            sgpd_result = GeoSeries(pt).z
            assert isinstance(sgpd_result, ps.Series)
            gpd_result = gpd.GeoSeries(pt).z
            self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_m(self):
        pass

    def test_from_file(self):
        pass

    def test_from_wkb(self):
        for geom in self.geoms:
            wkb = [g.wkb for g in geom]
            sgpd_result = GeoSeries.from_wkb(wkb)
            gpd_result = gpd.GeoSeries.from_wkb(wkb)
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_from_wkt(self):
        for geom in self.geoms:
            wkt = [g.wkt for g in geom]
            sgpd_result = GeoSeries.from_wkt(wkt)
            gpd_result = gpd.GeoSeries.from_wkt(wkt)
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_from_xy(self):
        tests = [
            [
                [2.5, 0.5, 5.0, -2],  # x
                [5, 10, 0, 1],  # y
                [-3, 1.5, -1000, 25],  # z
                "EPSG:4326",
            ],
            [
                [2.5, -0.5, 1, 500],  # x
                [5, 1, -100, 1000],  # y
                None,
                None,
            ],
        ]
        for x, y, z, crs in tests:
            sgpd_result = GeoSeries.from_xy(x, y, z, crs=crs)
            gpd_result = gpd.GeoSeries.from_xy(x, y, z, crs=crs)
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)
            assert sgpd_result.crs == gpd_result.crs

    def test_from_shapely(self):
        pass

    def test_from_arrow(self):
        if parse_version(gpd.__version__) < parse_version("1.0.0"):
            return

        for geom in self.geoms:
            gpd_series = gpd.GeoSeries(geom)
            gpd_result = gpd.GeoSeries.from_arrow(gpd_series.to_arrow())

            sgpd_result = GeoSeries.from_arrow(gpd_series.to_arrow())
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_to_file(self):
        pass

    @pytest.mark.parametrize("fun", ["isna", "isnull"])
    def test_isna(self, fun):
        for geom in self.geoms:
            sgpd_result = getattr(GeoSeries(geom), fun)()
            assert isinstance(sgpd_result, ps.Series)
            gpd_result = getattr(gpd.GeoSeries(geom), fun)()
            self.check_pd_series_equal(sgpd_result, gpd_result)

    @pytest.mark.parametrize("fun", ["notna", "notnull"])
    def test_notna(self, fun):
        for geom in self.geoms:
            sgpd_result = getattr(GeoSeries(geom), fun)()
            assert isinstance(sgpd_result, ps.Series)
            gpd_result = getattr(gpd.GeoSeries(geom), fun)()
            self.check_pd_series_equal(sgpd_result, gpd_result)

        data = [Point(0, 0), None]
        series = GeoSeries(data)
        sgpd_result = series.notna()
        gpd_result = gpd.GeoSeries(data).notna()
        self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_fillna(self):
        for geom in self.geoms:
            sgpd_result = GeoSeries(geom).fillna()
            gpd_result = gpd.GeoSeries(geom).fillna()
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

        data = [Point(1, 1), None, None, None, Point(0, 1)]
        sgpd_result = GeoSeries(data).fillna()
        gpd_result = gpd.GeoSeries(data).fillna()
        self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

        fill_data = [Point(-1, -1), Point(-2, -2), Point(2, 3)]
        sgpd_result = GeoSeries(data).fillna(GeoSeries(fill_data))
        gpd_result = gpd.GeoSeries(data).fillna(gpd.GeoSeries(fill_data))
        self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

        # Ensure filling with np.nan or pd.NA returns None
        # but filling None return empty geometry
        import numpy as np

        for fill_val in [np.nan, pd.NA, None]:
            sgpd_result = GeoSeries(data).fillna(fill_val)
            gpd_result = gpd.GeoSeries(data).fillna(fill_val)
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_explode(self):
        pass

    def test_to_crs(self):
        for geom in self.geoms:
            if isinstance(geom[0], Polygon) and geom[0] == Polygon():
                # SetSRID doesn't set SRID properly on empty polygon
                # https://github.com/apache/sedona/issues/2403
                # We replace it with a valid polygon as a workaround to pass the test
                geom[0] = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])

            sgpd_result = GeoSeries(geom, crs=4326).to_crs(epsg=3857)
            gpd_result = gpd.GeoSeries(geom, crs=4326).to_crs(epsg=3857)
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_bounds(self):
        for geom in self.geoms:
            sgpd_result = GeoSeries(geom).bounds
            gpd_result = gpd.GeoSeries(geom).bounds
            # This method returns a dataframe instead of a series
            pd.testing.assert_frame_equal(
                sgpd_result.to_pandas(), pd.DataFrame(gpd_result)
            )

    def test_total_bounds(self):
        import numpy as np

        for geom in self.geoms:
            sgpd_result = GeoSeries(geom).total_bounds
            gpd_result = gpd.GeoSeries(geom).total_bounds
            np.testing.assert_array_equal(sgpd_result, gpd_result)

    def test_estimate_utm_crs(self):
        for crs in ["epsg:4326", "epsg:3857"]:
            for geom in self.geoms:
                if isinstance(geom[0], Polygon) and geom[0] == Polygon():
                    # SetSRID doesn't set SRID properly on empty polygon
                    # https://github.com/apache/sedona/issues/2403
                    # We replace it with a valid polygon as a workaround to pass the test
                    geom[0] = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
                gpd_result = gpd.GeoSeries(geom, crs=crs).estimate_utm_crs()
                sgpd_result = GeoSeries(geom, crs=crs).estimate_utm_crs()
                assert sgpd_result == gpd_result

    def test_to_json(self):
        for geom in self.geoms:
            # Sedona converts it to LineString, so the outputs will be different
            if isinstance(geom[0], LinearRing):
                continue
            sgpd_result = GeoSeries(geom).to_json()
            gpd_result = gpd.GeoSeries(geom).to_json()
            assert sgpd_result == gpd_result

    def test_to_wkb(self):
        for geom in self.geoms:
            sgpd_result = GeoSeries(geom).to_wkb()
            gpd_result = gpd.GeoSeries(geom).to_wkb()
            self.check_pd_series_equal(sgpd_result, gpd_result)

            sgpd_result = GeoSeries(geom).to_wkb(hex=True)
            gpd_result = gpd.GeoSeries(geom).to_wkb(hex=True)
            self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_to_wkt(self):
        for geom in self.geoms:
            ps_series = GeoSeries(geom).to_wkt()
            pd_series = gpd.GeoSeries(geom).to_wkt()
            # There are slight variations of valid wkt (e.g valid parentheses being optional),
            # so we check that they can be interpreted as the same geometry rather than
            # their strings being exactly equal.
            sgpd_result = GeoSeries.from_wkt(ps_series)
            gpd_result = gpd.GeoSeries.from_wkt(pd_series)
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_to_arrow(self):
        if parse_version(gpd.__version__) < parse_version("1.0.0"):
            return

        import pyarrow as pa

        for geom in self.geoms:
            # LINEARRING EMPTY and LineString EMPTY
            # result in 01EA03000000000000 instead of 010200000000000000.
            # Sedona returns the right result, so this bug is likely in pyarrow or geoarrow
            # Below we set the modify the failing case as a workaround to pass the test
            # Occurs in python 3.9, but fixed by python 3.10.
            if geom[0] in [LineString(), LinearRing()]:
                geom[0] = LineString([(0, 0), (1, 1)])

            sgpd_result = pa.array(GeoSeries(geom).to_arrow())
            gpd_result = pa.array(gpd.GeoSeries(geom).to_arrow())
            assert sgpd_result == gpd_result

    def test_clip(self):
        pass

    def test_geom_type(self):
        for geom in self.geoms:
            # Sedona converts it to LineString, so the outputs will be different
            if isinstance(geom[0], LinearRing):
                continue
            sgpd_result = GeoSeries(geom).geom_type
            gpd_result = gpd.GeoSeries(geom).geom_type
            self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_type(self):
        pass

    def test_length(self):
        for geom in self.geoms:
            sgpd_result = GeoSeries(geom).length
            assert isinstance(sgpd_result, ps.Series)
            gpd_result = gpd.GeoSeries(geom).length
            self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_is_valid(self):
        for geom in self.geoms:
            sgpd_result = GeoSeries(geom).is_valid
            assert isinstance(sgpd_result, ps.Series)
            gpd_result = gpd.GeoSeries(geom).is_valid
            self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_is_valid_reason(self):
        # is_valid_reason was added in geopandas 1.0.0
        if parse_version(gpd.__version__) < parse_version("1.0.0"):
            pytest.skip("geopandas is_valid_reason requires version 1.0.0 or higher")

        data = [
            Polygon([(0, 0), (1, 1), (0, 1)]),
            Polygon([(0, 0), (1, 1), (1, 0), (0, 1)]),  # bowtie geometry
            Polygon([(0, 0), (2, 2), (2, 0)]),
            Polygon(
                [(0, 0), (2, 0), (1, 1), (2, 2), (0, 2), (1, 1), (0, 0)]
            ),  # ring intersection
            None,
        ]
        sgpd_result = GeoSeries(data).is_valid_reason()
        assert isinstance(sgpd_result, ps.Series)
        gpd_result = gpd.GeoSeries(data).is_valid_reason()
        for a, e in zip(sgpd_result.to_pandas(), gpd_result):
            if a is None and e is None:
                continue
            if a == "Valid Geometry":
                assert e == "Valid Geometry"
            elif "Self-intersection" in a:
                assert "Self-intersection" in e
            else:
                raise ValueError(f"Unexpected result: {a} not equivalent to {e}")

    def test_is_empty(self):
        for geom in self.geoms:
            sgpd_result = GeoSeries(geom).is_empty
            assert isinstance(sgpd_result, ps.Series)
            gpd_result = gpd.GeoSeries(geom).is_empty
            self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_count_coordinates(self):
        pass

    def test_count_geometries(self):
        pass

    def test_count_interior_rings(self):
        pass

    def test_dwithin(self):
        if parse_version(gpd.__version__) < parse_version("1.0.0"):
            pytest.skip("geopandas < 1.0.0 does not support dwithin")

        for geom, geom2 in self.pairs:
            sgpd_result = GeoSeries(geom).dwithin(GeoSeries(geom2), distance=1)
            gpd_result = gpd.GeoSeries(geom).dwithin(gpd.GeoSeries(geom2), distance=1)
            self.check_pd_series_equal(sgpd_result, gpd_result)

            if len(geom) == len(geom2):
                sgpd_result = GeoSeries(geom).dwithin(
                    GeoSeries(geom2), distance=1, align=False
                )
                gpd_result = gpd.GeoSeries(geom).dwithin(
                    gpd.GeoSeries(geom2), distance=1, align=False
                )
                self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_difference(self):
        for geom, geom2 in self.pairs:
            # Sedona doesn't support difference for GeometryCollections
            if isinstance(geom[0], GeometryCollection) or isinstance(
                geom2[0], GeometryCollection
            ):
                continue
            # Operation doesn't work on invalid geometries
            if (
                not gpd.GeoSeries(geom).is_valid.all()
                or not gpd.GeoSeries(geom2).is_valid.all()
            ):
                continue

            sgpd_result = GeoSeries(geom).difference(GeoSeries(geom2))
            gpd_result = gpd.GeoSeries(geom).difference(gpd.GeoSeries(geom2))
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

            if len(geom) == len(geom2):
                sgpd_result = GeoSeries(geom).difference(GeoSeries(geom2), align=False)
                gpd_result = gpd.GeoSeries(geom).difference(
                    gpd.GeoSeries(geom2), align=False
                )
                self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_symmetric_difference(self):
        for geom, geom2 in self.pairs:
            # Operation doesn't work on invalid geometries
            if (
                not gpd.GeoSeries(geom).is_valid.all()
                or not gpd.GeoSeries(geom2).is_valid.all()
            ):
                continue

            sgpd_result = GeoSeries(geom).symmetric_difference(GeoSeries(geom2))
            gpd_result = gpd.GeoSeries(geom).symmetric_difference(gpd.GeoSeries(geom2))
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

            if len(geom) == len(geom2):
                sgpd_result = GeoSeries(geom).symmetric_difference(
                    GeoSeries(geom2), align=False
                )
                gpd_result = gpd.GeoSeries(geom).symmetric_difference(
                    gpd.GeoSeries(geom2), align=False
                )
                self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_union(self):
        for geom, geom2 in self.pairs:
            # Operation doesn't work on invalid geometries
            if (
                not gpd.GeoSeries(geom).is_valid.all()
                or not gpd.GeoSeries(geom2).is_valid.all()
            ):
                continue

            sgpd_result = GeoSeries(geom).union(GeoSeries(geom2))
            gpd_result = gpd.GeoSeries(geom).union(gpd.GeoSeries(geom2))
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

            if len(geom) == len(geom2):
                sgpd_result = GeoSeries(geom).union(GeoSeries(geom2), align=False)
                gpd_result = gpd.GeoSeries(geom).union(
                    gpd.GeoSeries(geom2), align=False
                )
                self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_is_simple(self):
        # 'is_simple' is meaningful only for `LineStrings` and `LinearRings`
        data = [
            LineString([(0, 0), (0, 0)]),
            LineString([(0, 0), (1, 1), (1, -1), (0, 1)]),
            LineString([(0, 0), (1, 1), (0, 0)]),
            LinearRing([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)]),
            LinearRing([(0, 0), (-1, 1), (-1, -1), (1, -1)]),
        ]
        sgpd_result = GeoSeries(data).is_simple
        gpd_result = gpd.GeoSeries(data).is_simple
        self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_is_ring(self):
        # is_ring is only meaningful for linestrings so we use self.linestrings instead of self.geoms
        for geom in self.linestrings:
            sgpd_result = GeoSeries(geom).is_ring
            gpd_result = gpd.GeoSeries(geom).is_ring
            self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_is_ccw(self):
        pass

    def test_is_closed(self):
        if parse_version(gpd.__version__) < parse_version("1.0.0"):
            pytest.skip("geopandas is_closed requires version 1.0.0 or higher")
        # Test all geometry types to ensure non-LineString/LinearRing geometries return False
        for geom in self.geoms:
            # Geopandas returns True for LINEARRING EMPTY, but Sedona can't detect linear rings
            # so we skip this case
            if isinstance(geom[0], LinearRing):
                continue
            sgpd_result = GeoSeries(geom).is_closed
            gpd_result = gpd.GeoSeries(geom).is_closed
            self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_has_z(self):
        for geom in self.geoms:
            sgpd_result = GeoSeries(geom).has_z
            gpd_result = gpd.GeoSeries(geom).has_z
            self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_get_precision(self):
        pass

    def test_get_geometry(self):
        if parse_version(gpd.__version__) < parse_version("1.0.0"):
            pytest.skip("geopandas get_geometry requires version 1.0.0 or higher")

        for geom in self.geoms:
            # test negative index, in-bounds index, and out of bounds index
            for index in [-1, 0, len(geom) + 1]:
                sgpd_result = GeoSeries(geom).get_geometry(index)
                gpd_result = gpd.GeoSeries(geom).get_geometry(index)
                self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

        data = [GeometryCollection(), Polygon(), MultiPolygon()]

        for idx in [-2, -1, 0, 1]:
            sgpd_result = GeoSeries(data).get_geometry(idx)
            gpd_result = gpd.GeoSeries(data).get_geometry(idx)
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_boundary(self):
        for geom in self.geoms:
            # Shapely < 2.0 doesn't support GeometryCollection for boundary operation
            if shapely.__version__ < "2.0.0" and isinstance(
                geom[0], GeometryCollection
            ):
                continue
            sgpd_result = GeoSeries(geom).boundary
            gpd_result = gpd.GeoSeries(geom).boundary
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_centroid(self):
        for geom in self.geoms:
            sgpd_result = GeoSeries(geom).centroid
            gpd_result = gpd.GeoSeries(geom).centroid
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_concave_hull(self):
        pass

    def test_convex_hull(self):
        pass

    def test_delaunay_triangles(self):
        pass

    def test_voronoi_polygons(self):
        pass

    def test_envelope(self):
        for geom in self.geoms:
            sgpd_result = GeoSeries(geom).envelope
            gpd_result = gpd.GeoSeries(geom).envelope
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_minimum_rotated_rectangle(self):
        pass

    def test_exterior(self):
        pass

    def test_extract_unique_points(self):
        pass

    def test_offset_curve(self):
        pass

    def test_interiors(self):
        pass

    def test_remove_repeated_points(self):
        pass

    def test_set_precision(self):
        pass

    def test_representative_point(self):
        pass

    def test_minimum_bounding_circle(self):
        pass

    def test_minimum_bounding_radius(self):
        pass

    def test_minimum_clearance(self):
        pass

    def test_normalize(self):
        pass

    def test_make_valid(self):
        import shapely

        # 'structure' method requires shapely >= 2.1.0
        if shapely.__version__ < "2.1.0":
            pytest.skip("geopandas make_valid requires shapely >= 2.1.0")

        for geom in self.geoms:
            sgpd_result = GeoSeries(geom).make_valid(method="structure")
            gpd_result = gpd.GeoSeries(geom).make_valid(method="structure")
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

        for geom in self.geoms:
            sgpd_result = GeoSeries(geom).make_valid(
                method="structure", keep_collapsed=False
            )
            gpd_result = gpd.GeoSeries(geom).make_valid(
                method="structure", keep_collapsed=False
            )
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

        # Ensure default method="linework" fails
        with pytest.raises(ValueError):
            GeoSeries([Point(0, 0)]).make_valid()
        with pytest.raises(ValueError):
            GeoSeries([Point(0, 0)]).make_valid(method="linework")

    def test_reverse(self):
        pass

    @pytest.mark.skipif(
        parse_version(gpd.__version__) < parse_version("0.14.0"),
        reason="geopandas segmentize requires version 0.14.0 or higher",
    )
    def test_segmentize(self):
        for geom in self.geoms:
            sgpd_result = GeoSeries(geom).segmentize(2.5)
            gpd_result = gpd.GeoSeries(geom).segmentize(2.5)
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_transform(self):
        pass

    def test_force_2d(self):
        pass

    def test_force_3d(self):
        pass

    def test_line_merge(self):
        pass

    def test_unary_union(self):
        pass

    def test_union_all(self):
        if parse_version(gpd.__version__) < parse_version("1.1.0"):
            pytest.skip("geopandas union_all requires version 1.1.0 or higher")

        # Union all the valid geometries
        # Neither our nor geopandas' implementation supports invalid geometries
        lst = [g for geom in self.geoms for g in geom if g.is_valid]
        sgpd_result = GeoSeries(lst).union_all()
        gpd_result = gpd.GeoSeries(lst).union_all()
        self.check_geom_equals(sgpd_result, gpd_result)

        # Ensure we have the same result for empty GeoSeries
        sgpd_result = GeoSeries([]).union_all()
        gpd_result = gpd.GeoSeries([]).union_all()
        self.check_geom_equals(sgpd_result, gpd_result)

    def test_crosses(self):
        for geom, geom2 in self.pairs:
            if self.contains_any_geom_collection(geom, geom2):
                continue

            # We explicitly specify align=True to quite warnings in geopandas, despite it being the default
            gpd_result = gpd.GeoSeries(geom).crosses(gpd.GeoSeries(geom2), align=True)
            sgpd_result = GeoSeries(geom).crosses(GeoSeries(geom2), align=True)
            self.check_pd_series_equal(sgpd_result, gpd_result)

            if len(geom) == len(geom2):
                sgpd_result = GeoSeries(geom).crosses(GeoSeries(geom2), align=False)
                gpd_result = gpd.GeoSeries(geom).crosses(
                    gpd.GeoSeries(geom2), align=False
                )
                self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_disjoint(self):
        pass

    def test_intersects(self):
        for geom, geom2 in self.pairs:
            sgpd_result = GeoSeries(geom).intersects(GeoSeries(geom2), align=True)
            gpd_result = gpd.GeoSeries(geom).intersects(
                gpd.GeoSeries(geom2), align=True
            )
            self.check_pd_series_equal(sgpd_result, gpd_result)

            if len(geom) == len(geom2):
                sgpd_result = GeoSeries(geom).intersects(GeoSeries(geom2), align=False)
                gpd_result = gpd.GeoSeries(geom).intersects(
                    gpd.GeoSeries(geom2), align=False
                )
                self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_intersection(self):
        geometries = [
            Polygon([(0, 0), (1, 0), (1, 1)]),
            Polygon([(2, 0), (3, 0), (3, 1)]),
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(0, 0), (3, 0), (3, 3), (0, 2)]),
            Polygon([(2, 0), (3, 0), (3, 3), (2, 3)]),
            Point(0, 0),
        ]

        # Ensure resulting index behavior is correct for align=False (retain the left's index)
        index1 = range(1, len(geometries) + 1)
        index2 = range(len(geometries))
        sgpd_result = GeoSeries(geometries, index1).intersection(
            GeoSeries(geometries, index2), align=False
        )
        sgpd_result.sort_index(inplace=True)

        gpd_result = gpd.GeoSeries(geometries, index1).intersection(
            gpd.GeoSeries(geometries, index2), align=False
        )
        self.check_sgpd_equals_gpd(sgpd_result, gpd_result)
        assert sgpd_result.index.to_pandas().equals(gpd_result.index)

        # Ensure both align True and False work correctly
        for geom, geom2 in self.pairs:
            gpd_series1, gpd_series2 = gpd.GeoSeries(geom), gpd.GeoSeries(geom2)
            # The original geopandas intersection method fails on invalid geometries
            if not gpd_series1.is_valid.all() or not gpd_series2.is_valid.all():
                continue
            sgpd_result = GeoSeries(geom).intersection(GeoSeries(geom2))
            sgpd_result.sort_index(inplace=True)
            gpd_result = gpd_series1.intersection(gpd_series2)
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

            if len(geom) == len(geom2):
                sgpd_result = GeoSeries(geom).intersection(
                    GeoSeries(geom2), align=False
                )
                sgpd_result.sort_index(inplace=True)
                gpd_result = gpd_series1.intersection(gpd_series2, align=False)
                self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_snap(self):
        if parse_version(gpd.__version__) < parse_version("1.0.0"):
            return

        # Sedona's snap result fails fairly often, even though the results are fairly close.
        # (though in a way where increasing the buffer tolerance wouldn't help with)
        # Instead of testing all self.pairs, we test a few specific cases that are known to succeed
        # currently, just so we can catch regressions

        tests = [
            (self.linestrings, self.multipoints, 1.1, True),
            (self.linestrings, self.multipoints, 1, False),
            (self.linearrings, self.multilinestrings, 1, False),
        ]

        for geom, geom2, tol, align in tests:
            sgpd_result = GeoSeries(geom).snap(GeoSeries(geom2), tol, align=align)
            gpd_result = gpd.GeoSeries(geom).snap(
                gpd.GeoSeries(geom2), tol, align=align
            )
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_intersection_all(self):
        pass

    def test_overlaps(self):
        for geom, geom2 in self.pairs:
            # Sedona's results differ from geopandas for these cases
            if geom == geom2 or self.contains_any_geom_collection(geom, geom2):
                continue

            sgpd_result = GeoSeries(geom).overlaps(GeoSeries(geom2, align=True))
            gpd_result = gpd.GeoSeries(geom).overlaps(gpd.GeoSeries(geom2), align=True)
            self.check_pd_series_equal(sgpd_result, gpd_result)

            if len(geom) == len(geom2):
                sgpd_result = GeoSeries(geom).overlaps(GeoSeries(geom2), align=False)
                gpd_result = gpd.GeoSeries(geom).overlaps(
                    gpd.GeoSeries(geom2), align=False
                )
                self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_touches(self):
        for geom, geom2 in self.pairs:
            if self.contains_any_geom_collection(geom, geom2):
                continue
            sgpd_result = GeoSeries(geom).touches(GeoSeries(geom2), align=True)
            gpd_result = gpd.GeoSeries(geom).touches(gpd.GeoSeries(geom2), align=True)
            self.check_pd_series_equal(sgpd_result, gpd_result)

            if len(geom) == len(geom2):
                sgpd_result = GeoSeries(geom).touches(GeoSeries(geom2), align=False)
                gpd_result = gpd.GeoSeries(geom).touches(
                    gpd.GeoSeries(geom2), align=False
                )
                self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_within(self):
        for geom, geom2 in self.pairs:
            if geom == geom2 or self.contains_any_geom_collection(geom, geom2):
                continue

            sgpd_result = GeoSeries(geom).within(GeoSeries(geom2), align=True)
            gpd_result = gpd.GeoSeries(geom).within(gpd.GeoSeries(geom2), align=True)

            self.check_pd_series_equal(sgpd_result, gpd_result)

            if len(geom) == len(geom2):
                sgpd_result = GeoSeries(geom).within(GeoSeries(geom2), align=False)
                gpd_result = gpd.GeoSeries(geom).within(
                    gpd.GeoSeries(geom2), align=False
                )
                self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_covers(self):
        if parse_version(gpd.__version__) < parse_version("0.8.0"):
            pytest.skip("geopandas < 0.8.0 does not support covered_by")

        for geom, geom2 in self.pairs:
            if geom == geom2 or self.contains_any_geom_collection(geom, geom2):
                continue

            sgpd_result = GeoSeries(geom).covers(GeoSeries(geom2), align=True)
            gpd_result = gpd.GeoSeries(geom).covers(gpd.GeoSeries(geom2), align=True)
            self.check_pd_series_equal(sgpd_result, gpd_result)

            if len(geom) == len(geom2):
                sgpd_result = GeoSeries(geom).covers(GeoSeries(geom2), align=False)
                gpd_result = gpd.GeoSeries(geom).covers(
                    gpd.GeoSeries(geom2), align=False
                )
                self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_covered_by(self):
        if parse_version(shapely.__version__) < parse_version("2.0.0"):
            pytest.skip("shapely < 2.0.0 does not support covered_by")

        for geom, geom2 in self.pairs:
            if geom == geom2 or self.contains_any_geom_collection(geom, geom2):
                continue

            sgpd_result = GeoSeries(geom).covered_by(GeoSeries(geom2), align=True)
            gpd_result = gpd.GeoSeries(geom).covered_by(
                gpd.GeoSeries(geom2), align=True
            )
            self.check_pd_series_equal(sgpd_result, gpd_result)

            if len(geom) == len(geom2):
                sgpd_result = GeoSeries(geom).covered_by(GeoSeries(geom2), align=False)
                gpd_result = gpd.GeoSeries(geom).covered_by(
                    gpd.GeoSeries(geom2), align=False
                )
                self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_distance(self):
        for geom, geom2 in self.pairs:
            sgpd_result = GeoSeries(geom).distance(GeoSeries(geom2), align=True)
            gpd_result = gpd.GeoSeries(geom).distance(gpd.GeoSeries(geom2), align=True)
            self.check_pd_series_equal(sgpd_result, gpd_result)

            if len(geom) == len(geom2):
                sgpd_result = GeoSeries(geom).distance(GeoSeries(geom2), align=False)
                gpd_result = gpd.GeoSeries(geom).distance(
                    gpd.GeoSeries(geom2), align=False
                )
                self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_contains(self):
        for geom, geom2 in self.pairs:
            if geom == geom2 or self.contains_any_geom_collection(geom, geom2):
                continue
            sgpd_result = GeoSeries(geom).contains(GeoSeries(geom2), align=True)
            gpd_result = gpd.GeoSeries(geom).contains(gpd.GeoSeries(geom2), align=True)
            self.check_pd_series_equal(sgpd_result, gpd_result)

            if len(geom) == len(geom2):
                sgpd_result = GeoSeries(geom).contains(GeoSeries(geom2), align=False)
                gpd_result = gpd.GeoSeries(geom).contains(
                    gpd.GeoSeries(geom2), align=False
                )
                self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_contains_properly(self):
        pass

    def test_relate(self):
        for geom, geom2 in self.pairs:
            sgpd_result = GeoSeries(geom).relate(GeoSeries(geom2), align=True)
            gpd_result = gpd.GeoSeries(geom).relate(gpd.GeoSeries(geom2), align=True)
            self.check_pd_series_equal(sgpd_result, gpd_result)

            if len(geom) == len(geom2):
                sgpd_result = GeoSeries(geom).relate(GeoSeries(geom2), align=False)
                gpd_result = gpd.GeoSeries(geom).relate(
                    gpd.GeoSeries(geom2), align=False
                )
                self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_set_crs(self):
        for geom in self.geoms:
            if isinstance(geom[0], Polygon) and geom[0] == Polygon():
                # SetSRID doesn't set SRID properly on empty polygon
                # https://github.com/apache/sedona/issues/2403
                # We replace it with a valid polygon as a workaround to pass the test
                geom[0] = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
            sgpd_series = GeoSeries(geom)
            gpd_series = gpd.GeoSeries(geom)
            assert sgpd_series.crs == gpd_series.crs

            sgpd_series = sgpd_series.set_crs(epsg=4326)
            gpd_series = gpd_series.set_crs(epsg=4326)
            assert sgpd_series.crs == gpd_series.crs

            sgpd_series = sgpd_series.set_crs(epsg=3857, allow_override=True)
            gpd_series = gpd_series.set_crs(epsg=3857, allow_override=True)
            assert sgpd_series.crs == gpd_series.crs

        # Ensure we have the same result for empty GeoSeries
        sgpd_series = GeoSeries([])
        gpd_series = gpd.GeoSeries([])
        assert sgpd_series.crs == gpd_series.crs
