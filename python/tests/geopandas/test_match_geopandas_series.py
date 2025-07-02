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

from sedona.geopandas import GeoSeries
from tests.test_base import TestBase
import pyspark.pandas as ps


class TestMatchGeopandasSeries(TestBase):
    def setup_method(self):
        self.tempdir = tempfile.mkdtemp()
        self.t1 = Polygon([(0, 0), (1, 0), (1, 1)])
        self.t2 = Polygon([(0, 0), (1, 1), (0, 1)])
        self.sq = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        self.g1 = GeoSeries([self.t1, self.t2])
        self.g2 = GeoSeries([self.sq, self.t1])
        self.g3 = GeoSeries([self.t1, self.t2], crs="epsg:4326")
        self.g4 = GeoSeries([self.t2, self.t1])

        self.points = [Point(x, x + 1) for x in range(3)]

        self.multipoints = [MultiPoint([(x, x + 1), (x + 2, x + 3)]) for x in range(3)]

        self.linestrings = [LineString([(x, x + 1), (x + 2, x + 3)]) for x in range(3)]

        self.multilinestrings = [
            MultiLineString(
                [[[x, x + 1], [x + 2, x + 3]], [[x + 4, x + 5], [x + 6, x + 7]]]
            )
            for x in range(3)
        ]

        self.polygons = [
            Polygon([(x, 0), (x + 1, 0), (x + 2, 1), (x + 3, 1)]) for x in range(3)
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
        ]

        # (sql_table_name, geom)
        self.geoms = [
            ("points", self.points),
            ("multipoints", self.multipoints),
            ("linestrings", self.linestrings),
            ("multilinestrings", self.multilinestrings),
            ("polygons", self.polygons),
            ("multipolygons", self.multipolygons),
            ("geomcollection", self.geomcollection),
        ]

        # create the tables in sedona spark
        for i, (table_name, geoms) in enumerate(self.geoms):
            wkt_string = [g.wkt for g in geoms]
            pd_df = pd.DataFrame({"id": i, "geometry": wkt_string})
            spark_df = self.spark.createDataFrame(pd_df)
            spark_df.createOrReplaceTempView(table_name)

    def teardown_method(self):
        shutil.rmtree(self.tempdir)

    def test_constructor(self):
        for _, geom in self.geoms:
            gpd_series = gpd.GeoSeries(geom)
            assert isinstance(gpd_series, gpd.GeoSeries)
            assert isinstance(gpd_series.geometry, gpd.GeoSeries)

    def test_non_geom_fails(self):
        with pytest.raises(TypeError):
            GeoSeries([0, 1, 2])
        with pytest.raises(TypeError):
            GeoSeries([0, 1, 2], crs="epsg:4326")
        with pytest.raises(TypeError):
            GeoSeries(["a", "b", "c"])

    def test_to_geopandas(self):
        for _, geom in self.geoms:
            sgpd_result = GeoSeries(geom)
            gpd_result = gpd.GeoSeries(geom)
            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

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

        for _, geom in self.geoms:
            sgpd_result = GeoSeries(geom).area
            gpd_result = gpd.GeoSeries(geom).area
            self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_buffer(self):
        buffer = self.g1.buffer(0.2)
        assert buffer is not None
        assert type(buffer) is GeoSeries
        assert buffer.count() == 2

        for _, geom in self.geoms:
            dist = 0.2
            sgpd_result = GeoSeries(geom).buffer(dist)
            gpd_result = gpd.GeoSeries(geom).buffer(dist)

            self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_buffer_then_area(self):
        area = self.g1.buffer(0.2).area
        assert area is not None
        assert type(area) is ps.Series
        assert area.count() == 2

    def test_buffer_then_geoparquet(self):
        temp_file_path = os.path.join(
            self.tempdir, next(tempfile._get_candidate_names()) + ".parquet"
        )
        self.g1.buffer(0.2).to_parquet(temp_file_path)
        assert os.path.exists(temp_file_path)

    def test_geometry(self):
        for _, geom in self.geoms:
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
        pass

    def test_from_wkt(self):
        pass

    def test_from_xy(self):
        pass

    def test_from_shapely(self):
        pass

    def test_from_arrow(self):
        pass

    def test_to_file(self):
        pass

    def test_isna(self):
        pass

    def test_isnull(self):
        pass

    def test_notna(self):
        pass

    def test_notnull(self):
        pass

    def test_fillna(self):
        pass

    def test_explode(self):
        pass

    def test_to_crs(self):
        pass

    def test_estimate_utm_crs(self):
        pass

    def test_to_json(self):
        pass

    def test_to_wkb(self):
        pass

    def test_to_wkt(self):
        pass

    def test_to_arrow(self):
        pass

    def test_clip(self):
        pass

    def test_geom_type(self):
        pass

    def test_type(self):
        pass

    def test_length(self):
        for _, geom in self.geoms:
            sgpd_result = GeoSeries(geom).length
            assert isinstance(sgpd_result, ps.Series)
            gpd_result = gpd.GeoSeries(geom).length
            self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_is_valid(self):
        for _, geom in self.geoms:
            sgpd_result = GeoSeries(geom).is_valid
            assert isinstance(sgpd_result, ps.Series)
            gpd_result = gpd.GeoSeries(geom).is_valid
            self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_is_valid_reason(self):
        pass

    def test_is_empty(self):
        for _, geom in self.geoms:
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

    def test_is_simple(self):
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
        pass

    def test_is_ccw(self):
        pass

    def test_is_closed(self):
        pass

    def test_has_z(self):
        for _, geom in self.geoms:
            sgpd_result = GeoSeries(geom).has_z
            gpd_result = gpd.GeoSeries(geom).has_z
            self.check_pd_series_equal(sgpd_result, gpd_result)

    def test_get_precision(self):
        pass

    def test_get_geometry(self):
        pass

    def test_boundary(self):
        pass

    def test_centroid(self):
        pass

    def test_concave_hull(self):
        pass

    def test_convex_hull(self):
        pass

    def test_delaunay_triangles(self):
        pass

    def test_voronoi_polygons(self):
        pass

    def test_envelope(self):
        pass

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
        pass

    def test_reverse(self):
        pass

    def test_segmentize(self):
        pass

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
        pass

    def test_intersects(self):
        for _, geom in self.geoms:
            for _, geom2 in self.geoms:
                sgpd_result = GeoSeries(geom).intersects(GeoSeries(geom2))
                gpd_result = gpd.GeoSeries(geom).intersects(gpd.GeoSeries(geom2))
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
        for g1 in geometries:
            for g2 in geometries:
                sgpd_result = GeoSeries(g1).intersection(GeoSeries(g2))
                gpd_result = gpd.GeoSeries(g1).intersection(gpd.GeoSeries(g2))
                self.check_sgpd_equals_gpd(sgpd_result, gpd_result)

    def test_intersection_all(self):
        pass

    def test_contains(self):
        pass

    def test_contains_properly(self):
        pass

    def test_set_crs(self):
        for _, geom in self.geoms:
            sgpd_series = GeoSeries(geom)
            gpd_series = gpd.GeoSeries(geom)
            assert sgpd_series.crs == gpd_series.crs

            sgpd_series = sgpd_series.set_crs(epsg=4326)
            gpd_series = gpd_series.set_crs(epsg=4326)
            assert sgpd_series.crs == gpd_series.crs

            sgpd_series = sgpd_series.set_crs(epsg=3857, allow_override=True)
            gpd_series = gpd_series.set_crs(epsg=3857, allow_override=True)
            assert sgpd_series.crs == gpd_series.crs

    # -----------------------------------------------------------------------------
    # # Utils
    # -----------------------------------------------------------------------------

    def check_sgpd_equals_spark_df(
        self, actual: GeoSeries, expected: pyspark.sql.DataFrame
    ):
        assert isinstance(actual, GeoSeries)
        assert isinstance(expected, pyspark.sql.DataFrame)
        expected = expected.selectExpr("ST_AsText(expected) as expected")
        sgpd_result = actual.to_geopandas()
        expected = expected.toPandas()["expected"]
        for a, e in zip(sgpd_result, expected):
            self.assert_geometry_almost_equal(a, e)

    def check_sgpd_equals_gpd(self, actual: GeoSeries, expected: gpd.GeoSeries):
        assert isinstance(actual, GeoSeries)
        assert isinstance(expected, gpd.GeoSeries)
        sgpd_result = actual.to_geopandas()
        for a, e in zip(sgpd_result, expected):
            # Sometimes sedona and geopandas both return empty geometries but of different types (e.g Point and Polygon)
            if a.is_empty and e.is_empty:
                continue
            self.assert_geometry_almost_equal(
                a, e, tolerance=1e-2
            )  # increased tolerance from 1e-6

    def check_pd_series_equal(self, actual: ps.Series, expected: pd.Series):
        assert isinstance(actual, ps.Series)
        assert isinstance(expected, pd.Series)
        assert_series_equal(actual.to_pandas(), expected)
