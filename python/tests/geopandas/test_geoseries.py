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

from decimal import Decimal
import typing
import warnings

import shapely
import numpy as np
import pytest
import pandas as pd
import geopandas as gpd
import pyspark.pandas as ps
import sedona.spark.geopandas as sgpd
from pyspark.pandas.internal import InternalFrame, NATURAL_ORDER_COLUMN_NAME
from pyspark.pandas.utils import scol_for
from pyspark.sql import functions as F
from sedona.spark.geopandas import GeoSeries, GeoDataFrame
from sedona.spark.geopandas.geoseries import _to_bool
from sedona.spark.sql import st_functions as stf
from tests.geopandas.test_geopandas_base import TestGeopandasBase
from shapely import wkt
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
import pytest
from packaging.version import parse as parse_version

requires_geopandas_shared_paths = pytest.mark.skipif(
    parse_version(gpd.__version__) < parse_version("1.0.0"),
    reason=f"Tests require geopandas>=1.0.0, but found v{gpd.__version__}",
)

requires_geopandas_geom_equals_identical = pytest.mark.skipif(
    parse_version(gpd.__version__) < parse_version("1.1.0")
    or parse_version(shapely.__version__) < parse_version("2.1.0"),
    reason=(
        "Tests require geopandas>=1.1.0 and shapely>=2.1.0, but found "
        f"geopandas {gpd.__version__} and shapely {shapely.__version__}"
    ),
)

requires_shapely_m_support = pytest.mark.skipif(
    parse_version(shapely.__version__) < parse_version("2.1.0")
    or getattr(shapely, "geos_version", (0, 0, 0)) < (3, 12, 0),
    reason=(
        "M and ZM geometry tests require shapely>=2.1.0 and GEOS>=3.12.0, "
        f"but found shapely {shapely.__version__} and GEOS "
        f"{getattr(shapely, 'geos_version_string', 'unknown')}"
    ),
)


@pytest.mark.skipif(
    parse_version(shapely.__version__) < parse_version("2.0.0"),
    reason=f"Tests require shapely>=2.0.0, but found v{shapely.__version__}",
)
class TestGeoSeries(TestGeopandasBase):
    def setup_method(self):
        super().setup_method()
        self.geoseries = sgpd.GeoSeries(
            [
                Point(2.3, -1),
                LineString([(0.5, 0), (0, -3)]),
                Polygon([(-1, -1), (-0.3, 5), (1, 1.2)]),
                GeometryCollection(
                    [
                        Point(2.3, -1),
                        LineString([(0.5, 0), (0, -3)]),
                        Polygon([(-1, -1), (-0.3, 5), (1, 1.2)]),
                    ]
                ),
            ]
        )

    def test_empty_list(self):
        s = sgpd.GeoSeries([])
        assert s.count() == 0

    def test_to_bool_fills_nullable_boolean_series(self):
        nullable = self.spark.createDataFrame(
            [(0, True), (1, None), (2, False)],
            "id long, value boolean",
        ).pandas_api(index_col="id")["value"]

        self.check_pd_series_equal(
            _to_bool(nullable),
            pd.Series(
                [True, False, False],
                index=pd.Index([0, 1, 2], name="id"),
                name="value",
            ),
        )
        self.check_pd_series_equal(
            _to_bool(nullable, default=True),
            pd.Series(
                [True, True, False],
                index=pd.Index([0, 1, 2], name="id"),
                name="value",
            ),
        )

    def test_non_geom_fails(self):
        with pytest.raises(TypeError):
            GeoSeries([0, 1, 2])
        with pytest.raises(TypeError):
            GeoSeries([0, 1, 2], crs="epsg:4326")
        with pytest.raises(TypeError):
            GeoSeries(["a", "b", "c"])
        with pytest.raises(TypeError):
            GeoSeries(pd.Series([0, 1, 2]), crs="epsg:4326")
        with pytest.raises(TypeError):
            GeoSeries(ps.Series([0, 1, 2]))

    @pytest.mark.parametrize(
        "obj",
        [
            [Point(x, x) for x in range(3)],
            pd.Series([Point(x, x) for x in range(3)]),
            gpd.GeoSeries([Point(x, x) for x in range(3)]),
        ],
    )
    def test_constructor(self, obj):
        sgpd_series = sgpd.GeoSeries(obj)
        assert isinstance(sgpd_series, sgpd.GeoSeries)

    def test_constructor_pandas_on_spark(self):
        obj = ps.Series([Point(x, x) for x in range(3)])
        sgpd_series = GeoSeries(obj)
        assert isinstance(sgpd_series, sgpd.GeoSeries)

    def test_to_geopandas(self):
        from geopandas.testing import assert_geoseries_equal

        data = [Point(x, x) for x in range(3)]
        index = [1, 2, 3]
        crs = "EPSG:3857"
        result = GeoSeries(data, index=index, crs=crs).to_geopandas()
        gpd_df = gpd.GeoSeries(data, index=index, crs=crs)
        assert_geoseries_equal(result, gpd_df)

    def test_to_spark_pandas(self):
        data = [Point(x, x) for x in range(3)]
        index = [1, 2, 3]
        crs = "EPSG:3857"
        result = GeoSeries(data, index=index, crs=crs).to_spark_pandas()
        ps_df = ps.Series(data, index=index)
        self.check_pd_series_equal(result, ps_df.to_pandas())

    def test_sindex(self):
        s = GeoSeries([Point(x, x) for x in range(5)])
        assert not s.has_sindex

        result = s.sindex.query(box(1, 1, 3, 3))
        expected = [Point(1, 1), Point(2, 2), Point(3, 3)]
        assert result == expected
        assert s.has_sindex

        result = s.sindex.query(box(1, 1, 3, 3), predicate="contains")
        expected = [Point(1, 1), Point(2, 2), Point(3, 3)]
        assert result == expected
        assert s.has_sindex

        # Check that it works with a GeoDataFrame
        gdf = s.to_geoframe()
        result = gdf.sindex.query(box(1, 1, 3, 3), predicate="contains")
        assert result == expected

        # This is challenging to support due to gdf.__setitem__ casting GeoSeries into pspd.Series
        # assert gdf.has_sindex

    def test_invalidate_sindex(self):
        geoseries = GeoSeries([Point(0, 0), None, Point(2, 2)])

        line = LineString([(1, 1), (3, 3)])
        result1 = geoseries.sindex.query(line)
        assert len(result1) == 1
        assert geoseries.has_sindex

        # Fill the None element with a new geometry that intersects with the line
        # This should invalidate the sindex
        geoseries.fillna(Point(1, 1), inplace=True)
        assert not geoseries.has_sindex

        result = geoseries.sindex.query(line)
        assert len(result) == 2

        # For set_crs, no need to invalidate the sindex
        geoseries.set_crs(4326, inplace=True)
        assert geoseries.has_sindex

    def test_plot(self):
        # Just make sure it doesn't error
        self.geoseries.plot()

    def test_area(self):
        result = self.geoseries.area
        expected = pd.Series([0.0, 0.0, 5.23, 5.23])
        self.check_pd_series_equal(result, expected)

        # Test that GeoDataFrame.area also works
        df_result = self.geoseries.to_geoframe().area
        self.check_pd_series_equal(result, df_result.to_pandas())

    def test_buffer(self):

        s = GeoSeries(
            [
                Point(0, 0),
                LineString([(1, -1), (1, 0), (2, 0), (2, 1)]),
                Polygon([(3, -1), (4, 0), (3, 1)]),
            ]
        )
        result = s.buffer(0.2)
        expected = [
            "POLYGON ((0.2 0, 0.1990369453344394 -0.0196034280659121, 0.1961570560806461 -0.0390180644032256, 0.1913880671464418 -0.0580569354508925, 0.1847759065022574 -0.076536686473018, 0.176384252869671 -0.0942793473651995, 0.1662939224605091 -0.1111140466039204, 0.1546020906725474 -0.1268786568327291, 0.1414213562373095 -0.1414213562373095, 0.1268786568327291 -0.1546020906725474, 0.1111140466039205 -0.1662939224605091, 0.0942793473651996 -0.176384252869671, 0.076536686473018 -0.1847759065022574, 0.0580569354508925 -0.1913880671464418, 0.0390180644032257 -0.1961570560806461, 0.0196034280659122 -0.1990369453344394, 0 -0.2, -0.0196034280659121 -0.1990369453344394, -0.0390180644032256 -0.1961570560806461, -0.0580569354508924 -0.1913880671464418, -0.076536686473018 -0.1847759065022574, -0.0942793473651996 -0.176384252869671, -0.1111140466039204 -0.1662939224605091, -0.1268786568327291 -0.1546020906725474, -0.1414213562373095 -0.1414213562373095, -0.1546020906725474 -0.1268786568327291, -0.1662939224605091 -0.1111140466039204, -0.176384252869671 -0.0942793473651996, -0.1847759065022574 -0.076536686473018, -0.1913880671464418 -0.0580569354508925, -0.1961570560806461 -0.0390180644032257, -0.1990369453344394 -0.0196034280659122, -0.2 0, -0.1990369453344394 0.0196034280659121, -0.1961570560806461 0.0390180644032257, -0.1913880671464418 0.0580569354508924, -0.1847759065022574 0.0765366864730179, -0.176384252869671 0.0942793473651995, -0.1662939224605091 0.1111140466039204, -0.1546020906725474 0.1268786568327291, -0.1414213562373095 0.1414213562373095, -0.1268786568327292 0.1546020906725474, -0.1111140466039204 0.1662939224605091, -0.0942793473651996 0.176384252869671, -0.0765366864730181 0.1847759065022573, -0.0580569354508925 0.1913880671464418, -0.0390180644032257 0.1961570560806461, -0.0196034280659121 0.1990369453344394, 0 0.2, 0.019603428065912 0.1990369453344394, 0.0390180644032257 0.1961570560806461, 0.0580569354508924 0.1913880671464418, 0.076536686473018 0.1847759065022573, 0.0942793473651995 0.176384252869671, 0.1111140466039204 0.1662939224605091, 0.1268786568327291 0.1546020906725474, 0.1414213562373095 0.1414213562373095, 0.1546020906725474 0.1268786568327292, 0.1662939224605091 0.1111140466039204, 0.176384252869671 0.0942793473651996, 0.1847759065022573 0.0765366864730181, 0.1913880671464418 0.0580569354508925, 0.1961570560806461 0.0390180644032258, 0.1990369453344394 0.0196034280659121, 0.2 0))",
            "POLYGON ((0.8 0, 0.8009630546655606 0.0196034280659122, 0.803842943919354 0.0390180644032257, 0.8086119328535583 0.0580569354508925, 0.8152240934977426 0.076536686473018, 0.823615747130329 0.0942793473651996, 0.8337060775394909 0.1111140466039204, 0.8453979093274526 0.1268786568327291, 0.8585786437626906 0.1414213562373095, 0.8731213431672709 0.1546020906725474, 0.8888859533960796 0.1662939224605091, 0.9057206526348005 0.176384252869671, 0.9234633135269821 0.1847759065022574, 0.9419430645491076 0.1913880671464418, 0.9609819355967744 0.1961570560806461, 0.9803965719340879 0.1990369453344394, 1 0.2, 1.8 0.2, 1.8 1, 1.8009630546655606 1.019603428065912, 1.803842943919354 1.0390180644032256, 1.8086119328535581 1.0580569354508924, 1.8152240934977426 1.076536686473018, 1.823615747130329 1.0942793473651995, 1.8337060775394909 1.1111140466039204, 1.8453979093274526 1.1268786568327291, 1.8585786437626906 1.1414213562373094, 1.8731213431672709 1.1546020906725474, 1.8888859533960796 1.1662939224605091, 1.9057206526348005 1.176384252869671, 1.923463313526982 1.1847759065022574, 1.9419430645491076 1.1913880671464419, 1.9609819355967744 1.196157056080646, 1.980396571934088 1.1990369453344394, 2 1.2, 2.019603428065912 1.1990369453344394, 2.039018064403226 1.196157056080646, 2.0580569354508924 1.1913880671464419, 2.076536686473018 1.1847759065022574, 2.0942793473651995 1.176384252869671, 2.1111140466039204 1.1662939224605091, 2.126878656832729 1.1546020906725474, 2.1414213562373097 1.1414213562373094, 2.1546020906725474 1.1268786568327291, 2.166293922460509 1.1111140466039204, 2.176384252869671 1.0942793473651995, 2.1847759065022574 1.076536686473018, 2.1913880671464416 1.0580569354508924, 2.1961570560806463 1.0390180644032256, 2.1990369453344396 1.019603428065912, 2.2 1, 2.2 0, 2.1990369453344396 -0.0196034280659121, 2.1961570560806463 -0.0390180644032256, 2.1913880671464416 -0.0580569354508925, 2.1847759065022574 -0.076536686473018, 2.176384252869671 -0.0942793473651995, 2.166293922460509 -0.1111140466039204, 2.1546020906725474 -0.1268786568327291, 2.1414213562373097 -0.1414213562373095, 2.126878656832729 -0.1546020906725474, 2.1111140466039204 -0.1662939224605091, 2.0942793473651995 -0.176384252869671, 2.076536686473018 -0.1847759065022574, 2.0580569354508924 -0.1913880671464418, 2.039018064403226 -0.1961570560806461, 2.019603428065912 -0.1990369453344394, 2 -0.2, 1.2 -0.2, 1.2 -1, 1.1990369453344394 -1.019603428065912, 1.196157056080646 -1.0390180644032256, 1.1913880671464419 -1.0580569354508924, 1.1847759065022574 -1.076536686473018, 1.176384252869671 -1.0942793473651995, 1.1662939224605091 -1.1111140466039204, 1.1546020906725474 -1.1268786568327291, 1.1414213562373094 -1.1414213562373094, 1.1268786568327291 -1.1546020906725474, 1.1111140466039204 -1.1662939224605091, 1.0942793473651995 -1.176384252869671, 1.076536686473018 -1.1847759065022574, 1.0580569354508924 -1.1913880671464419, 1.0390180644032256 -1.196157056080646, 1.019603428065912 -1.1990369453344394, 1 -1.2, 0.9803965719340879 -1.1990369453344394, 0.9609819355967744 -1.196157056080646, 0.9419430645491076 -1.1913880671464419, 0.9234633135269821 -1.1847759065022574, 0.9057206526348005 -1.176384252869671, 0.8888859533960796 -1.1662939224605091, 0.8731213431672709 -1.1546020906725474, 0.8585786437626906 -1.1414213562373094, 0.8453979093274526 -1.1268786568327291, 0.8337060775394909 -1.1111140466039204, 0.823615747130329 -1.0942793473651995, 0.8152240934977426 -1.076536686473018, 0.8086119328535583 -1.0580569354508924, 0.803842943919354 -1.0390180644032256, 0.8009630546655606 -1.019603428065912, 0.8 -1, 0.8 0))",
            "POLYGON ((2.8 -1, 2.8 1, 2.8009630546655604 1.019603428065912, 2.8038429439193537 1.0390180644032256, 2.8086119328535584 1.0580569354508924, 2.8152240934977426 1.076536686473018, 2.823615747130329 1.0942793473651995, 2.833706077539491 1.1111140466039204, 2.8453979093274526 1.1268786568327291, 2.8585786437626903 1.1414213562373094, 2.873121343167271 1.1546020906725474, 2.8888859533960796 1.1662939224605091, 2.9057206526348005 1.176384252869671, 2.923463313526982 1.1847759065022574, 2.9419430645491076 1.1913880671464419, 2.9609819355967746 1.196157056080646, 2.980396571934088 1.1990369453344394, 3 1.2, 3.019603428065912 1.1990369453344394, 3.039018064403226 1.196157056080646, 3.0580569354508924 1.1913880671464416, 3.076536686473018 1.1847759065022574, 3.0942793473651995 1.176384252869671, 3.1111140466039204 1.166293922460509, 3.126878656832729 1.1546020906725474, 3.1414213562373097 1.1414213562373094, 4.141421356237309 0.1414213562373095, 4.154602090672547 0.1268786568327292, 4.166293922460509 0.1111140466039206, 4.176384252869671 0.0942793473651996, 4.184775906502257 0.0765366864730181, 4.191388067146442 0.0580569354508926, 4.196157056080646 0.0390180644032257, 4.19903694533444 0.0196034280659121, 4.2 0, 4.19903694533444 -0.0196034280659121, 4.196157056080646 -0.0390180644032257, 4.191388067146442 -0.0580569354508926, 4.184775906502257 -0.076536686473018, 4.176384252869671 -0.0942793473651996, 4.166293922460509 -0.1111140466039206, 4.154602090672547 -0.1268786568327292, 4.141421356237309 -0.1414213562373095, 3.1414213562373097 -1.1414213562373094, 3.126878656832729 -1.1546020906725474, 3.1111140466039204 -1.166293922460509, 3.0942793473652 -1.1763842528696709, 3.076536686473018 -1.1847759065022574, 3.0580569354508924 -1.1913880671464416, 3.039018064403226 -1.196157056080646, 3.019603428065912 -1.1990369453344394, 3 -1.2, 2.980396571934088 -1.1990369453344394, 2.9609819355967746 -1.196157056080646, 2.9419430645491076 -1.1913880671464419, 2.923463313526982 -1.1847759065022574, 2.9057206526348005 -1.176384252869671, 2.8888859533960796 -1.1662939224605091, 2.873121343167271 -1.1546020906725474, 2.8585786437626908 -1.1414213562373097, 2.8453979093274526 -1.1268786568327291, 2.833706077539491 -1.1111140466039204, 2.823615747130329 -1.0942793473651995, 2.8152240934977426 -1.076536686473018, 2.8086119328535584 -1.0580569354508924, 2.8038429439193537 -1.0390180644032256, 2.8009630546655604 -1.019603428065912, 2.8 -1))",
        ]
        expected = gpd.GeoSeries([wkt.loads(wkt_str) for wkt_str in expected])
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().buffer(0.2)
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_simplify(self):
        s = GeoSeries([Point(0, 0).buffer(1), LineString([(0, 0), (1, 10), (0, 20)])])

        result = s.simplify(1)
        expected = gpd.GeoSeries(
            [Polygon([(0, 1), (0, -1), (-1, 0), (0, 1)]), LineString([(0, 0), (0, 20)])]
        )

        self.check_sgpd_equals_gpd(result, expected)

        result = s.simplify(1.2, preserve_topology=False)
        expected = gpd.GeoSeries([Polygon(), LineString([(0, 0), (0, 20)])])
        self.check_sgpd_equals_gpd(result, expected)

        s = GeoSeries([LineString([(0, 0), (1, 0.1), (2, 0)])])
        result = s.simplify(0.2)
        expected = gpd.GeoSeries([LineString([(0, 0), (2, 0)])])
        self.check_sgpd_equals_gpd(result, expected)

        result = s.simplify(0.2, preserve_topology=False)
        expected = gpd.GeoSeries(
            [
                LineString([(0, 0), (2, 0)]),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().simplify(0.2, preserve_topology=False)
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_geometry(self):
        sgpd_geoseries = sgpd.GeoSeries([Point(0, 0), Point(1, 1)])
        assert isinstance(sgpd_geoseries.geometry, sgpd.GeoSeries)
        self.check_pd_series_equal(sgpd_geoseries.geometry, sgpd_geoseries.to_pandas())

    def test_x(self):
        geoseries = sgpd.GeoSeries(
            [Point(0, -1, 2.5), Point(2.5, 0, -1), Point(-1, 2.5, 0), Point(-1, 0)]
        )
        result = geoseries.x
        expected = pd.Series([0, 2.5, -1, -1])
        self.check_pd_series_equal(result, expected)

    def test_y(self):
        geoseries = sgpd.GeoSeries(
            [Point(0, -1, 2.5), Point(2.5, 0, -1), Point(-1, 2.5, 0), Point(-1, 0)]
        )
        result = geoseries.y
        expected = pd.Series([-1, 0, 2.5, 0])
        self.check_pd_series_equal(result, expected)

    def test_z(self):
        geoseries = sgpd.GeoSeries(
            [Point(0, -1, 2.5), Point(2.5, 0, -1), Point(-1, 2.5, 0), Point(-1, 0)]
        )
        result = geoseries.z
        expected = pd.Series([2.5, -1, 0, np.nan])
        self.check_pd_series_equal(result, expected)

    def test_m(self):
        geoseries = sgpd.GeoSeries(
            [Point(0, -1, 2.5), Point(2.5, 0, -1), Point(-1, 2.5, 0)]
        )
        result = geoseries.m
        # ST_M returns NaN for points without M coordinate
        expected = pd.Series([np.nan, np.nan, np.nan])
        self.check_pd_series_equal(result, expected)

    def test_from_file(self):
        pass

    def test_from_wkb(self):
        wkbs = [
            (
                b"\x01\x01\x00\x00\x00\x00\x00\x00\x00"
                b"\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?"
            ),
            (
                b"\x01\x01\x00\x00\x00\x00\x00\x00\x00"
                b"\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00@"
            ),
            (
                b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00"
                b"\x00\x08@\x00\x00\x00\x00\x00\x00\x08@"
            ),
        ]
        s = sgpd.GeoSeries.from_wkb(wkbs)
        expected = gpd.GeoSeries([Point(1, 1), Point(2, 2), Point(3, 3)])
        self.check_sgpd_equals_gpd(s, expected)

    def test_from_wkt(self):
        wkts = [
            "POINT (1 1)",
            "POINT (2 2)",
            "POINT (3 3)",
        ]
        s = sgpd.GeoSeries.from_wkt(wkts)
        expected = gpd.GeoSeries([Point(1, 1), Point(2, 2), Point(3, 3)])
        self.check_sgpd_equals_gpd(s, expected)

    def test_from_xy(self):
        x = [2.5, 5, -3.0]
        y = [0.5, 1, 1.5]
        s = sgpd.GeoSeries.from_xy(x, y, crs="EPSG:4326")
        expected = gpd.GeoSeries([Point(2.5, 0.5), Point(5, 1), Point(-3, 1.5)])
        self.check_sgpd_equals_gpd(s, expected)

        z = [1, 2, 3]
        s = sgpd.GeoSeries.from_xy(x, y, z)
        expected = gpd.GeoSeries(
            [Point(2.5, 0.5, 1), Point(5, 1, 2), Point(-3, 1.5, 3)]
        )
        self.check_sgpd_equals_gpd(s, expected)

    def test_from_shapely(self):
        pass

    def test_from_arrow(self):
        if parse_version(gpd.__version__) < parse_version("1.0.0"):
            return

        import pyarrow as pa

        table = pa.table({"a": [0, 1, 2], "b": [0.1, 0.2, 0.3]})
        with pytest.raises(ValueError, match="No GeoArrow geometry field found"):
            GeoSeries.from_arrow(table["a"].chunk(0))

        gpd_series = gpd.GeoSeries(
            [Point(1, 1), Polygon(), LineString([(0, 0), (1, 1)]), None]
        )
        result = sgpd.GeoSeries.from_arrow(gpd_series.to_arrow())
        self.check_sgpd_equals_gpd(result, gpd_series)

    def test_to_file(self):
        pass

    @pytest.mark.parametrize("fun", ["isna", "isnull"])
    def test_isna(self, fun):
        geoseries = GeoSeries([Polygon([(0, 0), (1, 1), (0, 1)]), None, Polygon([])])
        result = getattr(geoseries, fun)()
        expected = pd.Series([False, True, False])
        self.check_pd_series_equal(result, expected)

    @pytest.mark.parametrize("fun", ["notna", "notnull"])
    def test_notna(self, fun):
        geoseries = GeoSeries([Polygon([(0, 0), (1, 1), (0, 1)]), None, Polygon([])])
        result = getattr(geoseries, fun)()
        expected = pd.Series([True, False, True])
        self.check_pd_series_equal(result, expected)

    def test_fillna(self):
        s = sgpd.GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                None,
                Polygon([(0, 0), (-1, 1), (0, -1)]),
            ]
        )
        result = s.fillna()
        expected = gpd.GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                GeometryCollection(),
                Polygon([(0, 0), (-1, 1), (0, -1)]),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        numeric_name = sgpd.GeoSeries(gpd.GeoSeries([Point(1, 1), None], name=0))
        numeric_name_result = numeric_name.fillna(Point(0, 0))
        numeric_name_expected = gpd.GeoSeries([Point(1, 1), Point(0, 0)], name=0)
        self.check_sgpd_equals_gpd(numeric_name_result, numeric_name_expected)
        result = s.fillna(Polygon([(0, 1), (2, 1), (1, 2)]))
        expected = gpd.GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                Polygon([(0, 1), (2, 1), (1, 2)]),
                Polygon([(0, 0), (-1, 1), (0, -1)]),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Check for GeoSeries fill value
        s_fill = sgpd.GeoSeries(
            [
                Point(0, 0),
                Point(1, 1),
                Point(2, 2),
            ]
        )
        result = s.fillna(s_fill)
        expected = gpd.GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                Point(1, 1),
                Polygon([(0, 0), (-1, 1), (0, -1)]),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Ensure filling with np.nan or pd.NA returns None
        # Also check that the name is preserved for fillna
        import numpy as np

        data = [Point(0, 0), None]
        for fill_val in [np.nan, pd.NA]:
            result = GeoSeries(data, name="geometry").fillna(fill_val)
            expected = gpd.GeoSeries(data, name="geometry")
            self.check_sgpd_equals_gpd(result, expected)

        # Ensure filling with None is empty GeometryCollection and not None
        # Also check that inplace works
        result = GeoSeries(data, name="geometry")
        result.fillna(None, inplace=True)
        expected = gpd.GeoSeries([Point(0, 0), GeometryCollection()], name="geometry")
        self.check_sgpd_equals_gpd(result, expected)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"index_parts": True},
            {"ignore_index": True},
            {"ignore_index": True, "index_parts": True},
        ],
    )
    def test_explode(self, kwargs):
        from geopandas.testing import assert_geoseries_equal

        geometries = [
            MultiPoint([(0, 0), (1, 1)]),
            Point(9, 9),
            GeometryCollection([Point(2, 2), MultiPoint([(3, 3), (4, 4)])]),
            Point(),
            Polygon(),
            MultiPoint(),
            GeometryCollection(),
            None,
        ]
        index = pd.Index(range(10, 18), name="feature_id")
        expected = gpd.GeoSeries(
            geometries,
            index=index,
            name="geometry",
            crs="EPSG:4326",
        ).explode(**kwargs)

        result = GeoSeries(
            geometries,
            index=index,
            name="geometry",
            crs="EPSG:4326",
        ).explode(**kwargs)
        actual = result.to_geopandas()

        assert_geoseries_equal(
            actual,
            expected,
            check_index_type=False,
            check_geom_type=True,
            check_crs=True,
        )
        pd.testing.assert_index_equal(actual.index, expected.index, exact=False)
        pd.testing.assert_series_equal(
            result.is_empty.to_pandas(), expected.is_empty, check_index_type=False
        )

        all_empty = GeoSeries(
            [MultiPoint(), GeometryCollection(), None],
            crs="EPSG:4326",
        ).explode(**kwargs)
        assert all_empty.crs is not None
        assert all_empty.crs.to_epsg() == 4326

    def test_explode_docstring_examples_are_syntactically_valid(self):
        import doctest

        examples = doctest.DocTestParser().get_examples(GeoSeries.explode.__doc__)
        for example in examples:
            compile(example.source, "<GeoSeries.explode>", "single")

    @pytest.mark.parametrize("name", ["__index_level_1__", "__INDEX_LEVEL_1__"])
    def test_explode_internal_name_collision(self, name):
        from geopandas.testing import assert_geoseries_equal

        geometries = [MultiPoint([(0, 0), (1, 1)])]
        series = GeoSeries(geometries)
        series.name = name
        result = series.explode(index_parts=True).to_geopandas()
        expected = gpd.GeoSeries(geometries, name=name).explode(index_parts=True)
        assert_geoseries_equal(result, expected, check_index_type=False)

    def test_to_crs(self):
        from pyproj import CRS

        geoseries = sgpd.GeoSeries(
            [Point(1, 1), Point(2, 2), Point(3, 3)], crs=4326, name="geometry"
        )
        assert isinstance(geoseries.crs, CRS) and geoseries.crs.to_epsg() == 4326
        result = geoseries.to_crs(3857)
        assert isinstance(result.crs, CRS) and result.crs.to_epsg() == 3857
        expected = gpd.GeoSeries(
            [
                Point(111319.49079327356, 111325.14286638486),
                Point(222638.98158654712, 222684.20850554455),
                Point(333958.4723798207, 334111.1714019597),
            ],
            crs=3857,
            name="geometry",
        )
        self.check_sgpd_equals_gpd(result, expected)

    def test_bounds(self):
        d = [
            Point(2, 1),
            Polygon([(0, 0), (1, 1), (1, 0)]),
            LineString([(0, 1), (1, 2)]),
            None,
        ]
        geoseries = sgpd.GeoSeries(d, crs="EPSG:4326")
        result = geoseries.bounds

        expected = pd.DataFrame(
            {
                "minx": [2.0, 0.0, 0.0, np.nan],
                "miny": [1.0, 0.0, 1.0, np.nan],
                "maxx": [2.0, 1.0, 1.0, np.nan],
                "maxy": [1.0, 1.0, 2.0, np.nan],
            }
        )
        pd.testing.assert_frame_equal(result.to_pandas(), expected)

        df_result = geoseries.to_geoframe().bounds
        pd.testing.assert_frame_equal(df_result.to_pandas(), expected)

    def test_total_bounds(self, monkeypatch):
        d = [
            Point(3, -1),
            Polygon([(0, 0), (1, 1), (1, 0)]),
            LineString([(0, 1), (1, 2)]),
            None,
        ]
        geoseries = sgpd.GeoSeries(d, crs="EPSG:4326")
        empty_geoseries = sgpd.GeoSeries([], crs="EPSG:4326")
        all_empty_geoseries = sgpd.GeoSeries([Point(), Polygon()], crs="EPSG:4326")

        spark_dataframe_type = type(geoseries._internal.spark_frame)
        original_first = spark_dataframe_type.first
        action_count = 0

        def counting_first(frame):
            nonlocal action_count
            action_count += 1
            return original_first(frame)

        # Spark 4 uses a classic DataFrame subclass that overrides ``first``.
        # Patch the runtime class so the assertion works across Spark versions.
        monkeypatch.setattr(spark_dataframe_type, "first", counting_first)

        result = geoseries.total_bounds
        expected = np.array([0.0, -1.0, 3.0, 2.0])
        np.testing.assert_array_equal(result, expected)
        assert action_count == 1

        action_count = 0
        df_result = geoseries.to_geoframe().total_bounds
        np.testing.assert_array_equal(df_result, expected)
        assert action_count == 1

        action_count = 0
        empty_result = empty_geoseries.total_bounds
        np.testing.assert_array_equal(empty_result, np.full(4, np.nan))
        assert action_count == 1

        action_count = 0
        all_empty_result = all_empty_geoseries.total_bounds
        np.testing.assert_array_equal(all_empty_result, np.full(4, np.nan))
        assert action_count == 1

    @pytest.mark.parametrize(
        ("level", "expected"),
        [
            (2, [0, 10, 15, 2]),
            (3, [0, 42, 63, 10]),
            (16, [0, 2863311530, 4294967295, 715827882]),
        ],
    )
    def test_hilbert_distance(self, level, expected):
        geometries = [
            Point(0, 0),
            Point(1, 1),
            Point(1, 0),
            Polygon([(0, 0), (0, 1), (1, 1), (1, 0)]),
        ]
        index = pd.MultiIndex.from_tuples(
            [("a", 1), ("a", 2), ("b", 1), ("b", 2)],
            names=["group", "feature_id"],
        )
        source = GeoSeries(
            geometries,
            index=index,
            name="source_geometry",
            crs="EPSG:4326",
        )

        result = source.hilbert_distance(
            total_bounds=(0, 0, 1, 1),
            level=level,
        )
        pd.testing.assert_series_equal(
            result.to_pandas(),
            pd.Series(
                expected,
                index=index,
                name="hilbert_distance",
                dtype="int64",
            ),
            check_index_type=False,
        )

        # GeoDataFrame delegates to the active geometry column, not to the
        # first geometry-typed column in the frame.
        if level == 2:
            frame = GeoDataFrame(
                gpd.GeoDataFrame(
                    {
                        "decoy": gpd.GeoSeries(
                            [Point(0, 0)] * len(geometries),
                            index=index,
                            crs="EPSG:4326",
                        ),
                        "active": gpd.GeoSeries(
                            geometries,
                            index=index,
                            crs="EPSG:4326",
                        ),
                    },
                    index=index,
                    geometry="active",
                    crs="EPSG:4326",
                )
            )
            pd.testing.assert_series_equal(
                frame.hilbert_distance(
                    total_bounds=(0, 0, 1, 1),
                    level=level,
                ).to_pandas(),
                pd.Series(
                    expected,
                    index=index,
                    name="hilbert_distance",
                    dtype="int64",
                ),
                check_index_type=False,
            )

    def test_hilbert_distance_uses_envelope_midpoint_extent(self):
        source = GeoSeries(
            [box(-100, -100, 100, 100), Point(10, 10)],
            index=pd.Index(["large", "point"], name="feature"),
        )

        # The inferred extent is (0, 0, 10, 10), based on envelope
        # midpoints. It is deliberately different from source.total_bounds.
        expected = pd.Series(
            [0, 2863311530],
            index=pd.Index(["large", "point"], name="feature"),
            name="hilbert_distance",
            dtype="int64",
        )
        pd.testing.assert_series_equal(
            source.hilbert_distance().to_pandas(),
            expected,
            check_index_type=False,
        )

        zero_width = GeoSeries([Point(0, 0), Point(0, 2), Point(0, 1)])
        pd.testing.assert_series_equal(
            zero_width.hilbert_distance(level=2).to_pandas(),
            pd.Series([0, 5, 3], name="hilbert_distance", dtype="int64"),
        )

    @pytest.mark.parametrize("total_bounds", [None, (0, 0, 1, 1)])
    def test_hilbert_distance_uses_one_native_summary_action(
        self,
        monkeypatch,
        total_bounds,
    ):
        source = GeoSeries([Point(0, 0), Point(1, 1)])
        spark_dataframe_type = type(source._internal.spark_frame)
        original_first = spark_dataframe_type.first
        action_count = 0

        def counting_first(frame):
            nonlocal action_count
            action_count += 1
            return original_first(frame)

        monkeypatch.setattr(spark_dataframe_type, "first", counting_first)

        result = source.hilbert_distance(total_bounds=total_bounds, level=3)
        assert action_count == 1

        spark_frame = result._internal.spark_frame
        if hasattr(spark_frame, "_jdf"):
            plan = spark_frame._jdf.queryExecution().optimizedPlan().toString()
            assert "BatchEvalPython" not in plan
            assert "ArrowEvalPython" not in plan
            assert "PythonUDF" not in plan

    def test_hilbert_distance_rejects_missing_and_empty_geometries(self):
        source = GeoSeries(
            [
                Point(0, 0),
                None,
                Point(),
                LineString(),
                Polygon(),
                MultiPoint(),
                MultiLineString(),
                MultiPolygon(),
                GeometryCollection(),
            ]
        )
        with pytest.raises(
            ValueError,
            match="cannot be computed on a GeoSeries with empty or missing",
        ):
            source.hilbert_distance(total_bounds=(0, 0, 1, 1))

        empty = GeoSeries([], crs="EPSG:4326")
        with pytest.raises(ValueError, match="cannot infer total bounds"):
            empty.hilbert_distance()

        explicit_empty = empty.hilbert_distance(total_bounds=(0, 0, 1, 1))
        assert explicit_empty.name == "hilbert_distance"
        assert explicit_empty.to_pandas().empty

    def test_hilbert_distance_validates_level(self):
        source = GeoSeries([Point(0, 0), Point(1, 1)])

        with pytest.raises(ValueError, match="Level out of range"):
            source.hilbert_distance(level=17)
        with pytest.raises(TypeError, match="level must be an integer"):
            source.hilbert_distance(level=1.5)

        pd.testing.assert_series_equal(
            source.hilbert_distance(level=0).to_pandas(),
            pd.Series([0, 0], name="hilbert_distance", dtype="int64"),
        )

    # These tests were taken directly from the TestEstimateUtmCrs class in the geopandas test suite
    # https://github.com/geopandas/geopandas/blob/main/geopandas/tests/test_array.py
    def test_estimate_utm_crs(self):
        from pyproj import CRS

        assert typing.get_type_hints(GeoSeries.estimate_utm_crs)["return"] is CRS

        # setup
        esb = Point(-73.9847, 40.7484)
        sol = Point(-74.0446, 40.6893)
        landmarks = sgpd.GeoSeries([esb, sol], crs="epsg:4326")

        # geographic
        assert landmarks.estimate_utm_crs() == CRS("EPSG:32618")
        assert landmarks.estimate_utm_crs("NAD83") == CRS("EPSG:26918")

        # projected
        assert landmarks.to_crs("EPSG:3857").estimate_utm_crs() == CRS("EPSG:32618")

        # antimeridian
        antimeridian = sgpd.GeoSeries(
            [
                Point(1722483.900174921, 5228058.6143420935),
                Point(4624385.494808555, 8692574.544944234),
            ],
            crs="EPSG:3851",
        )
        assert antimeridian.estimate_utm_crs() == CRS("EPSG:32760")

        # out of bounds
        with pytest.raises(RuntimeError, match="Unable to determine UTM CRS"):
            sgpd.GeoSeries(
                [Polygon([(0, 90), (1, 90), (2, 90)])], crs="EPSG:4326"
            ).estimate_utm_crs()

        # missing crs
        with pytest.raises(RuntimeError, match="crs must be set"):
            sgpd.GeoSeries([Polygon([(0, 90), (1, 90), (2, 90)])]).estimate_utm_crs()

    def test_to_json(self):
        s = GeoSeries([Point(1, 1), Point(2, 2), Point(3, 3)])

        result = s.to_json()
        expected = '{"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "pr\
operties": {}, "geometry": {"type": "Point", "coordinates": [1.0, 1.0]}, "bbox": [1.0,\
 1.0, 1.0, 1.0]}, {"id": "1", "type": "Feature", "properties": {}, "geometry": {"type"\
: "Point", "coordinates": [2.0, 2.0]}, "bbox": [2.0, 2.0, 2.0, 2.0]}, {"id": "2", "typ\
e": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [3.0, 3.\
0]}, "bbox": [3.0, 3.0, 3.0, 3.0]}], "bbox": [1.0, 1.0, 3.0, 3.0]}'

        assert result == expected

        result = s.to_json(show_bbox=True)
        expected = '{"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [1.0, 1.0]}, "bbox": [1.0, 1.0, 1.0, 1.0]}, {"id": "1", "type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [2.0, 2.0]}, "bbox": [2.0, 2.0, 2.0, 2.0]}, {"id": "2", "type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [3.0, 3.0]}, "bbox": [3.0, 3.0, 3.0, 3.0]}], "bbox": [1.0, 1.0, 3.0, 3.0]}'
        assert result == expected

        result = s.to_json(drop_id=True)
        expected = '{"type": "FeatureCollection", "features": [{"type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [1.0, 1.0]}, "bbox": [1.0, 1.0, 1.0, 1.0]}, {"type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [2.0, 2.0]}, "bbox": [2.0, 2.0, 2.0, 2.0]}, {"type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [3.0, 3.0]}, "bbox": [3.0, 3.0, 3.0, 3.0]}], "bbox": [1.0, 1.0, 3.0, 3.0]}'
        assert result == expected

        result = s.set_crs("EPSG:3857").to_json(to_wgs84=True)
        expected = '{"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [8.983152841195214e-06, 8.983152841195177e-06]}, "bbox": [8.983152841195214e-06, 8.983152841195177e-06, 8.983152841195214e-06, 8.983152841195177e-06]}, {"id": "1", "type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [1.7966305682390428e-05, 1.7966305682390134e-05]}, "bbox": [1.7966305682390428e-05, 1.7966305682390134e-05, 1.7966305682390428e-05, 1.7966305682390134e-05]}, {"id": "2", "type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [2.6949458523585642e-05, 2.694945852358465e-05]}, "bbox": [2.6949458523585642e-05, 2.694945852358465e-05, 2.6949458523585642e-05, 2.694945852358465e-05]}], "bbox": [8.983152841195214e-06, 8.983152841195177e-06, 2.6949458523585642e-05, 2.694945852358465e-05]}'
        assert result == expected

    def test_to_wkb(self):
        if parse_version(shapely.__version__) < parse_version("2.0.0"):
            return

        data = [
            Point(0, 0),
            Polygon(),
            Polygon([(0, 0), (1, 1), (1, 0)]),
            None,
        ]
        result = sgpd.GeoSeries(data).to_wkb()
        expected = pd.Series(
            [
                b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                b"\x01\x03\x00\x00\x00\x00\x00\x00\x00",
                b"\x01\x03\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                None,
            ]
        )

        self.check_pd_series_equal(result, expected)

        result = sgpd.GeoSeries(data).to_wkb(hex=True)
        expected = pd.Series(
            [
                "010100000000000000000000000000000000000000",
                "010300000000000000",
                "0103000000010000000400000000000000000000000000000000000000000000000000F03F000000000000F03F000000000000F03F000000000000000000000000000000000000000000000000",
                None,
            ]
        )
        self.check_pd_series_equal(result, expected)

    def test_to_wkt(self):
        s = GeoSeries([Point(1, 1), Point(2, 2), Point(3, 3)])
        result = s.to_wkt()
        expected = pd.Series(
            [
                "POINT (1 1)",
                "POINT (2 2)",
                "POINT (3 3)",
            ]
        )
        self.check_pd_series_equal(result, expected)

        s = GeoSeries(
            [
                Polygon(),
                Point(1, 2),
                LineString([(0, 0), (1, 1)]),
                None,
            ]
        )
        result = s.to_wkt()

        # Old versions return empty GeometryCollection instead of empty Polygon
        if parse_version(shapely.__version__) < parse_version("2.0.0"):
            return

        expected = pd.Series(
            [
                "POLYGON EMPTY",
                "POINT (1 2)",
                "LINESTRING (0 0, 1 1)",
                None,
            ]
        )
        self.check_pd_series_equal(result, expected)

    def test_to_arrow(self):
        if parse_version(gpd.__version__) < parse_version("1.0.0"):
            return

        import pyarrow as pa

        gser = GeoSeries([Point(1, 2), Point(2, 1)])
        arrow_array = gser.to_arrow()
        result = pa.array(arrow_array)

        expected = [
            "0101000000000000000000F03F0000000000000040",
            "01010000000000000000000040000000000000F03F",
        ]
        expected = pa.array([bytes.fromhex(x) for x in expected], type=pa.binary())

        assert result.equals(expected)

    def test_clip(self):
        pass

    def test_clip_by_rect(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
                LineString([(0, 0), (2, 2)]),
                Point(0.5, 0.5),
                Point(5, 5),
                None,
            ],
        )
        result = s.clip_by_rect(0, 0, 1, 1)
        expected = gpd.GeoSeries(
            [
                Polygon([(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]),
                LineString([(0, 0), (1, 1)]),
                Point(0.5, 0.5),
                Polygon(),  # Sedona returns POLYGON EMPTY for non-intersecting
                None,
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().clip_by_rect(0, 0, 1, 1)
        self.check_sgpd_equals_gpd(df_result, expected)

        # Test invalid input types
        with pytest.raises(TypeError):
            s.clip_by_rect("a", 0, 1, 1)

    def test_geom_type(self):
        geoseries = sgpd.GeoSeries(
            [
                Point(0, 0),
                MultiPoint([Point(0, 0), Point(1, 1)]),
                LineString([(0, 0), (1, 1)]),
                MultiLineString(
                    [LineString([(0, 0), (1, 1)]), LineString([(2, 2), (3, 3)])]
                ),
                Polygon([(0, 0), (1, 0), (0, 1)]),
                MultiPolygon(
                    [
                        Polygon([(0, 0), (1, 0), (0, 1)]),
                        Polygon([(2, 2), (3, 2), (2, 3)]),
                    ]
                ),
                GeometryCollection([Point(0, 0), LineString([(0, 0), (1, 1)])]),
                LinearRing([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)]),
                None,
            ]
        )
        result = geoseries.geom_type
        expected = pd.Series(
            [
                "Point",
                "MultiPoint",
                "LineString",
                "MultiLineString",
                "Polygon",
                "MultiPolygon",
                "GeometryCollection",
                "LineString",  # Note: Sedona returns LineString instead of LinearRing
                None,
            ]
        )
        self.check_pd_series_equal(result, expected)

        df_result = geoseries.to_geoframe().geom_type
        self.check_pd_series_equal(df_result, expected)

    def test_type(self):
        geoseries = GeoSeries(
            [
                Point(0, 0),
                MultiPoint([(0, 0), (1, 1)]),
                LineString([(0, 0), (1, 1)]),
                MultiLineString([[(0, 0), (1, 1)]]),
                Polygon([(0, 0), (1, 0), (1, 1)]),
                MultiPolygon(
                    [
                        Polygon([(0, 0), (1, 0), (0, 1)]),
                        Polygon([(2, 2), (3, 2), (2, 3)]),
                    ]
                ),
                GeometryCollection([Point(0, 0), LineString([(0, 0), (1, 1)])]),
            ]
        )
        result = geoseries.type
        expected = pd.Series(
            [
                "Point",
                "MultiPoint",
                "LineString",
                "MultiLineString",
                "Polygon",
                "MultiPolygon",
                "GeometryCollection",
            ]
        )
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = geoseries.to_geoframe().type
        self.check_pd_series_equal(df_result, expected)

    def test_length(self):
        geoseries = GeoSeries(
            [
                Point(0, 0),
                LineString([(0, 0), (1, 1)]),
                Polygon([(0, 0), (1, 0), (1, 1)]),
                GeometryCollection(
                    [
                        Point(0, 0),
                        LineString([(0, 0), (1, 1)]),
                        Polygon([(0, 0), (1, 0), (1, 1)]),
                    ]
                ),
            ]
        )
        result = geoseries.length
        expected = pd.Series([0.000000, 1.414214, 3.414214, 4.828427])
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = geoseries.to_geoframe().length
        self.check_pd_series_equal(df_result, expected)

        # Ensure M-dimension doesn't break things.
        s = GeoSeries(
            [
                wkt.loads("POINT M (0 0 0)"),
                wkt.loads("LINESTRING M (0 0 0, 1 1 0)"),
                wkt.loads("POLYGON M ((0 0 0, 1 0 0, 1 1 0, 0 0 0))"),
                wkt.loads(
                    "GEOMETRYCOLLECTION M (POINT M (0 0 0), LINESTRING M (0 0 0, 1 1 0), POLYGON M ((0 0 0, 1 0 0, 1 1 0, 0 0 0)))"
                ),
            ]
        )
        result = s.length
        expected = pd.Series([0.000000, 1.414214, 3.414214, 4.828427])
        self.check_pd_series_equal(result, expected)

    def test_is_valid(self):
        geoseries = sgpd.GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                Polygon([(0, 0), (1, 1), (1, 0), (0, 1)]),  # bowtie geometry
                Polygon([(0, 0), (2, 2), (2, 0)]),
                None,
            ]
        )
        result = geoseries.is_valid
        expected = pd.Series([True, False, True, False])
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = geoseries.to_geoframe().is_valid
        self.check_pd_series_equal(df_result, expected)

    def test_is_valid_reason(self):
        s = sgpd.GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                Polygon([(0, 0), (1, 1), (1, 0), (0, 1)]),  # bowtie geometry
                Polygon([(0, 0), (2, 2), (2, 0)]),
                Polygon(
                    [(0, 0), (2, 0), (1, 1), (2, 2), (0, 2), (1, 1), (0, 0)]
                ),  # ring intersection
                None,
            ]
        )
        result = s.is_valid_reason()
        expected = pd.Series(
            [
                "Valid Geometry",
                "Self-intersection at or near point (0.5, 0.5, NaN)",
                "Valid Geometry",
                "Ring Self-intersection at or near point (1.0, 1.0)",
                None,
            ]
        )
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().is_valid_reason()
        self.check_pd_series_equal(df_result, expected)

    def test_is_empty(self):
        geoseries = sgpd.GeoSeries(
            [Point(), Point(2, 1), Polygon([(0, 0), (1, 1), (0, 1)]), None],
        )

        result = geoseries.is_empty
        expected = pd.Series([True, False, False, False])
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = geoseries.to_geoframe().is_empty
        self.check_pd_series_equal(df_result, expected)

    def test_count_coordinates(self):
        s = GeoSeries(
            [
                Point(0, 0),
                LineString([(0, 0), (1, 1), (2, 2)]),
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            ]
        )
        result = s.count_coordinates()
        expected = pd.Series([1, 3, 5], dtype="int32")
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().count_coordinates()
        self.check_pd_series_equal(df_result, expected)

    def test_get_coordinates(self):
        geometries = [
            Point(1, 1),
            LineString([(1, -1), (1, 0)]),
            Polygon(
                [(3, -1), (4, 0), (3, 1)],
                [[(3.2, -0.5), (3.5, 0), (3.2, 0.5)]],
            ),
            MultiPoint([(5, 5), (6, 6)]),
            MultiLineString([[(7, 7), (8, 8)], [(9, 9), (10, 10)]]),
            MultiPolygon(
                [
                    Polygon([(11, 11), (12, 11), (11, 12)]),
                    Polygon([(13, 13), (14, 13), (13, 14)]),
                ]
            ),
            GeometryCollection([Point(15, 15), LineString([(16, 16), (17, 17)])]),
            Point(),
            None,
        ]
        index = pd.Index([90, 80, 70, 60, 50, 40, 30, 20, 10], name="feature_id")
        expected_series = gpd.GeoSeries(geometries, index=index)
        actual_series = GeoSeries(expected_series)

        options = [
            {},
            {"index_parts": True},
            {"ignore_index": True},
            {"ignore_index": True, "index_parts": True},
        ]
        for kwargs in options:
            actual = actual_series.get_coordinates(**kwargs)
            expected = expected_series.get_coordinates(**kwargs)
            assert isinstance(actual, ps.DataFrame)
            pd.testing.assert_frame_equal(actual.to_pandas(), expected)

        dataframe_result = actual_series.to_geoframe().get_coordinates()
        pd.testing.assert_frame_equal(
            dataframe_result.to_pandas(), expected_series.get_coordinates()
        )

        coordinate_named_index = gpd.GeoSeries(
            [Point(1, 2), Point(3, 4)],
            index=pd.Index([10, 20], name="x"),
        )
        pd.testing.assert_frame_equal(
            GeoSeries(coordinate_named_index).get_coordinates().to_pandas(),
            coordinate_named_index.get_coordinates(),
        )

        empty_series = gpd.GeoSeries(
            [Point(), GeometryCollection(), None],
            index=pd.Index([3, 2, 1], name="feature_id"),
        )
        actual_empty = GeoSeries(empty_series).get_coordinates(index_parts=True)
        expected_empty = empty_series.get_coordinates(index_parts=True)
        # Spark keeps a fixed integer schema for the coordinate-position level,
        # including when no rows are produced. GeoPandas infers object only for
        # that all-empty level.
        pd.testing.assert_frame_equal(
            actual_empty.to_pandas(), expected_empty, check_index_type=False
        )

    def test_get_coordinates_multi_index(self):
        index = pd.MultiIndex.from_tuples(
            [("b", 2), ("a", 1), ("b", 1)], names=["group", "feature_id"]
        )
        geometries = [
            LineString([(0, 0), (1, 1)]),
            Point(2, 2),
            Polygon([(3, 3), (4, 3), (3, 4)]),
        ]
        expected_series = gpd.GeoSeries(geometries, index=index)
        actual_series = GeoSeries(expected_series)

        for kwargs in ({}, {"index_parts": True}, {"ignore_index": True}):
            actual = actual_series.get_coordinates(**kwargs).to_pandas()
            expected = expected_series.get_coordinates(**kwargs)
            pd.testing.assert_frame_equal(actual, expected)

    def test_get_coordinates_z(self):
        geometries_wkt = [
            "POINT (0 1)",
            "POINT Z (2 3 4)",
        ]
        expected_series = gpd.GeoSeries.from_wkt(geometries_wkt)
        actual_series = GeoSeries.from_wkt(geometries_wkt)

        actual = actual_series.get_coordinates(include_z=True).to_pandas()
        expected = expected_series.get_coordinates(include_z=True)
        pd.testing.assert_frame_equal(actual, expected)

    @pytest.mark.skipif(
        parse_version(shapely.__version__) < parse_version("2.1.0"),
        reason="M coordinates require shapely>=2.1.0",
    )
    def test_get_coordinates_m(self):
        geometries_wkt = [
            "POINT (0 1)",
            "POINT Z (2 3 4)",
            "POINT M (5 6 7)",
            "POINT ZM (8 9 10 11)",
        ]
        expected_series = gpd.GeoSeries.from_wkt(geometries_wkt)
        actual_series = GeoSeries.from_wkt(geometries_wkt)

        for kwargs in (
            {"include_m": True},
            {"include_z": True, "include_m": True},
        ):
            actual = actual_series.get_coordinates(**kwargs).to_pandas()
            expected = expected_series.get_coordinates(**kwargs)
            pd.testing.assert_frame_equal(actual, expected)

    def test_count_geometries(self):
        s = GeoSeries(
            [
                Point(0, 0),
                MultiPoint([(0, 0), (1, 1)]),
                MultiLineString([[(0, 0), (1, 1)], [(2, 2), (3, 3)]]),
            ]
        )
        result = s.count_geometries()
        expected = pd.Series([1, 2, 2], dtype="int32")
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().count_geometries()
        self.check_pd_series_equal(df_result, expected)

    def test_count_interior_rings(self):
        s = GeoSeries(
            [
                Polygon(
                    [(0, 0), (10, 0), (10, 10), (0, 10)],
                    [[(1, 1), (2, 1), (2, 2), (1, 2)]],
                ),
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            ]
        )
        result = s.count_interior_rings()
        expected = pd.Series([1, 0], dtype="int32")
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().count_interior_rings()
        self.check_pd_series_equal(df_result, expected)

    def test_dwithin(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                LineString([(0, 0), (0, 2)]),
                LineString([(0, 0), (0, 1)]),
                Point(0, 1),
            ],
            index=range(0, 4),
        )
        s2 = GeoSeries(
            [
                Polygon([(1, 0), (4, 2), (2, 2)]),
                Polygon([(2, 0), (3, 2), (2, 2)]),
                LineString([(2, 0), (2, 2)]),
                Point(1, 1),
            ],
            index=range(1, 5),
        )

        result = s2.dwithin(Point(0, 1), 1.8)
        expected = pd.Series([True, False, False, True], index=range(1, 5))
        self.check_pd_series_equal(result, expected)

        result = s.dwithin(s2, distance=1, align=True)
        expected = pd.Series([False, True, False, False, False])
        self.check_pd_series_equal(result, expected)

        result = s.dwithin(s2, distance=1, align=False)
        expected = pd.Series([True, False, False, True])
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().dwithin(s2, distance=1, align=False)
        self.check_pd_series_equal(df_result, expected)

    def test_dwithin_array_like_distances(self):
        index = pd.Index(["a", "b", "c"], name="feature_id")
        source = GeoSeries(
            [Point(0, 0), Point(2, 0), Point(5, 0)],
            index=index,
        )
        expected = pd.Series([True, False, True], index=index)

        distances = [
            [0, 1, 5],
            (0, 1, 5),
            np.array([0, 1, 5]),
            pd.Series([0, 1, 5], index=["unrelated-c", "unrelated-a", "unrelated-b"]),
        ]
        for distance in distances:
            result = source.dwithin(Point(0, 0), distance)
            self.check_pd_series_equal(result, expected)

        broadcast_expected = pd.Series([True, True, False], index=index)
        for distance in ([3], (3,), np.array([3]), np.array(3), np.int64(3)):
            result = source.dwithin(Point(0, 0), distance)
            self.check_pd_series_equal(result, broadcast_expected)

    def test_dwithin_distances_follow_geometry_alignment(self):
        index_name = "feature_id"
        left = GeoSeries(
            [Point(100, 0), Point(0, 0), Point(0, 0)],
            index=pd.Index(["b", "a", "c"], name=index_name),
        )
        right = GeoSeries(
            [Point(2, 0), Point(2, 0), Point(9, 0)],
            index=pd.Index(["c", "a", "d"], name=index_name),
        )

        aligned_expected = pd.Series(
            [False, False, True, False],
            index=pd.Index(["a", "b", "c", "d"], name=index_name),
        )
        aligned = left.dwithin(right, [1, 99, 2, 99], align=True)
        self.check_pd_series_equal(aligned, aligned_expected)

        with pytest.warns(UserWarning, match="indices of the left and right"):
            default_aligned = left.dwithin(right, [1, 99, 2, 99])
        self.check_pd_series_equal(default_aligned, aligned_expected)

        positional_expected = pd.Series(
            [False, True, False],
            index=pd.Index(["b", "a", "c"], name=index_name),
        )
        positional = left.dwithin(right, [1, 99, 2], align=False)
        self.check_pd_series_equal(positional, positional_expected)

        with pytest.raises(Exception, match="distance array must contain"):
            left.dwithin(right, [1, 99, 2], align=True).to_pandas()
        with pytest.raises(Exception, match="distance array must contain"):
            left.dwithin(right, [1, 99, 2, 99], align=False).to_pandas()

    def test_dwithin_distributed_distance_series(self):
        index = pd.Index(["a", "a", "b"], name="feature_id")
        source = GeoSeries(
            [Point(0, 0), Point(2, 0), Point(5, 0)],
            index=index,
        )
        expected = pd.Series([True, False, True], index=index)

        independent_distance = ps.Series(
            [0, 1, 5],
            index=pd.Index(["ignored-c", "ignored-a", "ignored-b"]),
        )
        independent = source.dwithin(Point(0, 0), independent_distance)
        self.check_pd_series_equal(independent, expected)

        same_anchor_frame = GeoDataFrame(
            gpd.GeoDataFrame(
                {
                    "geometry": [Point(0, 0), Point(2, 0), Point(5, 0)],
                    "distance": [0, 1, 5],
                },
                index=index,
            )
        )
        same_anchor = same_anchor_frame.geometry.dwithin(
            Point(0, 0),
            same_anchor_frame["distance"],
        )
        self.check_pd_series_equal(same_anchor, expected)
        if hasattr(same_anchor._internal.spark_frame, "_jdf"):
            same_anchor_plan = (
                same_anchor._internal.spark_frame._jdf.queryExecution()
                .executedPlan()
                .toString()
            )
            assert "AttachDistributedSequence" not in same_anchor_plan
            assert "Join" not in same_anchor_plan

        if hasattr(independent._internal.spark_frame, "_jdf"):
            plan = (
                independent._internal.spark_frame._jdf.queryExecution()
                .executedPlan()
                .toString()
            )
            assert "BatchEvalPython" not in plan
            assert "ArrowEvalPython" not in plan
            assert "PythonUDF" not in plan

    def test_dwithin_duplicate_multiindex_alignment(self):
        left_index = pd.MultiIndex.from_tuples(
            [("a", 1), ("a", 1), ("c", 3)],
            names=["group", "row"],
        )
        right_index = pd.MultiIndex.from_tuples(
            [("a", 1), ("a", 1), ("b", 2)],
            names=["group", "row"],
        )
        left = GeoSeries(
            [Point(0, 0), Point(10, 0), Point(50, 0)],
            index=left_index,
        )
        right = GeoSeries(
            [Point(1, 0), Point(12, 0), Point(60, 0)],
            index=right_index,
        )

        result = left.dwithin(
            right,
            [1, 11, 9, 1, 100, 100],
            align=True,
        )
        expected_index = pd.MultiIndex.from_tuples(
            [
                ("a", 1),
                ("a", 1),
                ("a", 1),
                ("a", 1),
                ("b", 2),
                ("c", 3),
            ],
            names=["group", "row"],
        )
        expected = pd.Series(
            [True, False, True, False, False, False],
            index=expected_index,
        )
        self.check_pd_series_equal(result, expected)

        scalar_result = left.dwithin(Point(0, 0), [0, 10, 49])
        scalar_expected = pd.Series([True, True, False], index=left_index)
        self.check_pd_series_equal(scalar_result, scalar_expected)

    def test_dwithin_special_values_and_distance_validation(self):
        source = GeoSeries([Point(), None, Point(2, 0), Point(0, 0), Point(0, 0)])
        result = source.dwithin(
            Point(0, 0),
            np.array([np.inf, np.inf, np.inf, np.nan, -1.0]),
        )
        self.check_pd_series_equal(
            result,
            pd.Series([False, False, True, False, False]),
        )

        for distances in ([], [1, 2], [1, 2, 3, 4, 5, 6], pd.Series([1])):
            with pytest.raises(Exception, match="distance array must contain"):
                source.dwithin(Point(0, 0), distances).to_pandas()

        for distances in (
            ps.Series([1, 2]),
            ps.Series([1, 2, 3, 4, 5, 6]),
        ):
            with pytest.raises(Exception, match="distance array must contain"):
                source.dwithin(Point(0, 0), distances).to_pandas()

        with pytest.raises(ValueError, match="one-dimensional"):
            source.dwithin(Point(0, 0), np.ones((5, 1)))
        with pytest.raises(TypeError, match="numeric"):
            source.dwithin(Point(0, 0), ["1", "2", "3", "4", "5"])
        with pytest.raises(TypeError, match="numeric"):
            source.dwithin(Point(0, 0), [1, 2, 3, 4, pd.NA])
        with pytest.raises(TypeError, match="numeric"):
            source.dwithin(
                Point(0, 0),
                pd.Series([1, 2, 3, 4, pd.NA], dtype="Float64"),
            )
        with pytest.raises(TypeError, match="numeric"):
            source.dwithin(Point(0, 0), Decimal("1"))
        with pytest.raises(TypeError, match="numeric"):
            source.dwithin(Point(0, 0), [1, 2, 3, 4, Decimal("5")])
        with pytest.raises(TypeError, match="numeric"):
            source.dwithin(Point(0, 0), ps.Series(["1"] * 5))
        with pytest.raises(TypeError, match="numeric"):
            source.dwithin(Point(0, 0), ps.Series([Decimal("1")] * 5))
        with pytest.raises(
            ValueError,
            match=r"Lengths of inputs do not match\. Left: 1, Right: 2",
        ):
            GeoSeries([Point(0, 0)]).dwithin(
                GeoSeries([Point(0, 0), Point(1, 1)]),
                1,
                align=False,
            )

        empty = GeoSeries([])
        for distances in ([], np.array([]), ps.Series([], dtype=float)):
            empty_result = empty.dwithin(Point(0, 0), distances)
            self.check_pd_series_equal(empty_result, pd.Series([], dtype=bool))

    def test_difference(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (2, 2), (0, 2)]),
                Polygon([(0, 0), (2, 2), (0, 2)]),
                LineString([(0, 0), (2, 2)]),
                LineString([(2, 0), (0, 2)]),
                Point(0, 1),
            ],
        )
        s2 = GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                LineString([(1, 0), (1, 3)]),
                LineString([(2, 0), (0, 2)]),
                Point(1, 1),
                Point(0, 1),
            ],
            index=range(1, 6),
        )

        result = s.difference(Polygon([(0, 0), (1, 1), (0, 1)]))
        expected = gpd.GeoSeries(
            [
                Polygon([(0, 2), (2, 2), (1, 1), (0, 1), (0, 2)]),
                Polygon([(0, 2), (2, 2), (1, 1), (0, 1), (0, 2)]),
                LineString([(1, 1), (2, 2)]),
                MultiLineString(
                    [LineString([(2, 0), (1, 1)]), LineString([(1, 1), (0, 2)])]
                ),
                Point(),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        result = s.difference(s2, align=True)
        expected = gpd.GeoSeries(
            [
                None,
                Polygon([(0, 2), (2, 2), (1, 1), (0, 1), (0, 2)]),
                MultiLineString(
                    [LineString([(0, 0), (1, 1)]), LineString([(1, 1), (2, 2)])]
                ),
                LineString(),
                Point(0, 1),
                None,
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        result = s.difference(s2, align=False)
        expected = gpd.GeoSeries(
            [
                Polygon([(0, 2), (2, 2), (1, 1), (0, 1), (0, 2)]),
                Polygon([(0, 0), (0, 2), (1, 2), (2, 2), (1, 1), (0, 0)]),
                MultiLineString(
                    [LineString([(0, 0), (1, 1)]), LineString([(1, 1), (2, 2)])]
                ),
                LineString([(2, 0), (0, 2)]),
                Point(),
            ]
        )

        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().difference(s2, align=False)
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_symmetric_difference(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (2, 2), (0, 2)]),
                Polygon([(0, 0), (2, 2), (0, 2)]),
                LineString([(0, 0), (2, 2)]),
                LineString([(2, 0), (0, 2)]),
                Point(0, 1),
            ],
        )
        s2 = GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                LineString([(1, 0), (1, 3)]),
                LineString([(2, 0), (0, 2)]),
                Point(1, 1),
                Point(0, 1),
            ],
            index=range(1, 6),
        )

        # Test with single geometry
        result = s.symmetric_difference(Polygon([(0, 0), (1, 1), (0, 1)]))
        expected = gpd.GeoSeries(
            [
                Polygon([(0, 2), (2, 2), (1, 1), (0, 1), (0, 2)]),
                Polygon([(0, 2), (2, 2), (1, 1), (0, 1), (0, 2)]),
                GeometryCollection(
                    [
                        Polygon([(0, 0), (0, 1), (1, 1), (0, 0)]),
                        LineString([(1, 1), (2, 2)]),
                    ]
                ),
                GeometryCollection(
                    [
                        Polygon([(0, 0), (0, 1), (1, 1), (0, 0)]),
                        LineString([(2, 0), (1, 1)]),
                        LineString([(1, 1), (0, 2)]),
                    ]
                ),
                Polygon([(0, 1), (1, 1), (0, 0), (0, 1)]),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Test with align=True
        result = s.symmetric_difference(s2, align=True)
        expected = gpd.GeoSeries(
            [
                None,
                Polygon([(0, 2), (2, 2), (1, 1), (0, 1), (0, 2)]),
                MultiLineString(
                    [
                        LineString([(0, 0), (1, 1)]),
                        LineString([(1, 1), (2, 2)]),
                        LineString([(1, 0), (1, 1)]),
                        LineString([(1, 1), (1, 3)]),
                    ]
                ),
                LineString(),
                MultiPoint([Point(0, 1), Point(1, 1)]),
                None,
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Test with align=False
        result = s.symmetric_difference(s2, align=False)
        expected = gpd.GeoSeries(
            [
                Polygon([(0, 2), (2, 2), (1, 1), (0, 1), (0, 2)]),
                GeometryCollection(
                    [
                        Polygon([(0, 0), (0, 2), (1, 2), (2, 2), (1, 1), (0, 0)]),
                        LineString([(1, 0), (1, 1)]),
                        LineString([(1, 1), (1, 3)]),
                    ]
                ),
                MultiLineString(
                    [
                        LineString([(0, 0), (1, 1)]),
                        LineString([(1, 1), (2, 2)]),
                        LineString([(2, 0), (1, 1)]),
                        LineString([(1, 1), (0, 2)]),
                    ]
                ),
                LineString([(2, 0), (0, 2)]),
                Point(),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().symmetric_difference(s2, align=False)
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_union(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (2, 2), (0, 2)]),
                Polygon([(0, 0), (2, 2), (0, 2)]),
                LineString([(0, 0), (2, 2)]),
                LineString([(2, 0), (0, 2)]),
                Point(0, 1),
            ],
        )
        s2 = GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                LineString([(1, 0), (1, 3)]),
                LineString([(2, 0), (0, 2)]),
                Point(1, 1),
                Point(0, 1),
            ],
            index=range(1, 6),
        )

        # Test with single geometry
        result = s.union(Polygon([(0, 0), (1, 1), (0, 1)]))
        expected = gpd.GeoSeries(
            [
                Polygon([(0, 0), (0, 1), (0, 2), (2, 2), (1, 1), (0, 0)]),
                Polygon([(0, 0), (0, 1), (0, 2), (2, 2), (1, 1), (0, 0)]),
                GeometryCollection(
                    [
                        Polygon([(0, 0), (0, 1), (1, 1), (0, 0)]),
                        LineString([(0, 0), (2, 2)]),
                    ]
                ),
                GeometryCollection(
                    [
                        Polygon([(0, 0), (0, 1), (1, 1), (0, 0)]),
                        LineString([(2, 0), (0, 2)]),
                    ]
                ),
                Polygon([(0, 1), (1, 1), (0, 0), (0, 1)]),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Test with align=True
        result = s.union(s2, align=True)
        expected = gpd.GeoSeries(
            [
                None,
                Polygon([(0, 0), (0, 1), (0, 2), (2, 2), (1, 1), (0, 0)]),
                MultiLineString(
                    [
                        LineString([(0, 0), (1, 1)]),
                        LineString([(1, 1), (2, 2)]),
                        LineString([(1, 0), (1, 1)]),
                        LineString([(1, 1), (1, 3)]),
                    ]
                ),
                LineString([(2, 0), (0, 2)]),
                MultiPoint([Point(0, 1), Point(1, 1)]),
                None,
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Test with align=False
        result = s.union(s2, align=False)
        expected = gpd.GeoSeries(
            [
                Polygon([(0, 0), (0, 1), (0, 2), (2, 2), (1, 1), (0, 0)]),
                GeometryCollection(
                    [
                        Polygon([(0, 0), (0, 2), (1, 2), (2, 2), (1, 1), (0, 0)]),
                        LineString([(1, 0), (1, 1)]),
                        LineString([(1, 1), (1, 3)]),
                    ]
                ),
                MultiLineString(
                    [
                        LineString([(0, 0), (1, 1)]),
                        LineString([(1, 1), (2, 2)]),
                        LineString([(2, 0), (1, 1)]),
                        LineString([(1, 1), (0, 2)]),
                    ]
                ),
                LineString([(2, 0), (0, 2)]),
                Point(0, 1),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().union(s2, align=False)
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_is_simple(self):
        s = sgpd.GeoSeries(
            [
                LineString([(0, 0), (1, 1), (1, -1), (0, 1)]),
                LineString([(0, 0), (1, 1), (1, -1)]),
                LinearRing([(0, 0), (1, 1), (1, -1), (0, 1)]),
                LinearRing([(0, 0), (-1, 1), (-1, -1), (1, -1)]),
            ]
        )
        result = s.is_simple
        expected = pd.Series([False, True, False, True])
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().is_simple
        self.check_pd_series_equal(df_result, expected)

    def test_is_ring(self):
        s = GeoSeries(
            [
                LineString([(0, 0), (1, 1), (1, -1)]),
                LineString([(0, 0), (1, 1), (1, -1), (0, 0)]),
                LinearRing([(0, 0), (1, 1), (1, -1)]),
            ]
        )
        result = s.is_ring
        expected = pd.Series([False, True, True])
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        result = s.to_geoframe().is_ring
        self.check_pd_series_equal(result, expected)

    def test_is_ccw(self):
        index = pd.Index(
            [
                "ccw-line",
                "cw-line",
                "ccw-open-line",
                "asymmetric-open-line",
                "open-three-point-line",
                "closed-three-point-line",
                "ccw-ring",
                "polygon",
                "point",
                "empty-line",
                "empty-polygon",
                "null",
            ],
            name="feature_id",
        )
        s = GeoSeries(
            [
                LineString([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]),
                LineString([(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)]),
                LineString([(0, 0), (1, 0), (1, 1), (0, 1)]),
                LineString([(0, 1), (0, -1), (-1, -2), (3, -2)]),
                LineString([(0, 0), (1, 0), (0, 1)]),
                LineString([(0, 0), (1, 0), (0, 0)]),
                LinearRing([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Polygon([(0, 0), (1, 0), (0, 1), (0, 0)]),
                Point(0, 0),
                LineString(),
                Polygon(),
                None,
            ],
            index=index,
        )
        expected = pd.Series(
            [
                True,
                False,
                True,
                False,
                False,
                False,
                True,
                False,
                False,
                False,
                False,
                False,
            ],
            index=index,
        )

        result = s.is_ccw
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too.
        frame_result = s.to_geoframe().is_ccw
        self.check_pd_series_equal(frame_result, expected)

    def test_is_closed(self):
        s = GeoSeries(
            [
                LineString([(0, 0), (1, 1), (1, -1)]),
                LineString([(0, 0), (1, 1), (1, -1), (0, 0)]),
                LinearRing([(0, 0), (1, 1), (1, -1)]),
            ]
        )
        result = s.is_closed
        expected = pd.Series([False, True, True])
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        result = s.to_geoframe().is_closed
        self.check_pd_series_equal(result, expected)

        s = GeoSeries(
            [
                wkt.loads("LINESTRING M (0 0 0, 1 1 0, 1 -1 0, 0 0 0)"),
            ]
        )
        result = s.is_closed
        expected = pd.Series([True])
        self.check_pd_series_equal(result, expected)

    def test_has_z(self):
        s = sgpd.GeoSeries(
            [
                Point(0, 1),
                Point(0, 1, 2),
                Polygon([(0, 0, 1), (0, 1, 2), (1, 1, 3), (0, 0, 1)]),
                Polygon([(0, 0), (0, 1), (1, 1), (0, 0)]),
            ]
        )
        result = s.has_z
        expected = pd.Series([False, True, True, False])
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().has_z
        self.check_pd_series_equal(df_result, expected)

        mixed = GeoSeries.from_wkt(
            [
                "GEOMETRYCOLLECTION (POINT (0 0), POINT Z (1 1 2))",
                (
                    "GEOMETRYCOLLECTION (POINT (0 0), "
                    "GEOMETRYCOLLECTION (POINT Z (1 1 2)))"
                ),
                "POINT EMPTY",
                None,
            ]
        )
        mixed_expected = pd.Series([True, True, False, False])
        self.check_pd_series_equal(mixed.has_z, mixed_expected)

    def test_has_m(self):
        s = GeoSeries.from_wkt(
            [
                "POINT (0 1)",
                "POINT Z (0 1 2)",
                "POINT M (0 1 2)",
                "POINT ZM (0 1 2 3)",
                "GEOMETRYCOLLECTION (POINT (0 0), POINT M (1 1 2))",
                "POINT EMPTY",
                None,
            ]
        )
        expected = pd.Series([False, False, True, True, True, False, False])

        result = s.has_m
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too.
        df_result = s.to_geoframe().has_m
        self.check_pd_series_equal(df_result, expected)

        indexed = GeoSeries(
            [Point(0, 0), None],
            index=pd.Index(["point", "null"], name="feature_id"),
        )
        indexed_expected = pd.Series(
            [False, False],
            index=pd.Index(["point", "null"], name="feature_id"),
        )
        self.check_pd_series_equal(indexed.has_m, indexed_expected)

    def test_get_precision(self):
        pass

    def test_get_geometry(self):
        # Shapely 1 seems to have a bug where Polygon() is incorrectly interpreted as a GeometryCollection
        if shapely.__version__ < "2.0.0":
            return

        from shapely.geometry import MultiPoint

        s = GeoSeries(
            [
                Point(0, 0),
                MultiPoint([(0, 0), (1, 1), (0, 1), (1, 0)]),
                GeometryCollection(
                    [MultiPoint([(0, 0), (1, 1), (0, 1), (1, 0)]), Point(0, 1)]
                ),
                Polygon(),
                GeometryCollection(),
            ]
        )

        result = s.get_geometry(0)
        expected = gpd.GeoSeries(
            [
                Point(0, 0),
                Point(0, 0),
                MultiPoint([(0, 0), (1, 1), (0, 1), (1, 0)]),
                Polygon(),
                None,
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        result = s.get_geometry(1)
        expected = gpd.GeoSeries([None, Point(1, 1), Point(0, 1), None, None])
        self.check_sgpd_equals_gpd(result, expected)

        result = s.get_geometry(-1)
        expected = gpd.GeoSeries(
            [Point(0, 0), Point(1, 0), Point(0, 1), Polygon(), None]
        )
        self.check_sgpd_equals_gpd(result, expected)

        result = s.get_geometry(2)
        expected = gpd.GeoSeries([None, Point(0, 1), None, None, None])
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().get_geometry(2)
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_boundary(self):
        s = sgpd.GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                LineString([(0, 0), (1, 1), (1, 0)]),
                Point(0, 0),
                GeometryCollection([Point(0, 0)]),
            ]
        )
        result = s.boundary
        expected = gpd.GeoSeries(
            [
                LineString([(0, 0), (1, 1), (0, 1), (0, 0)]),
                MultiPoint([(0, 0), (1, 0)]),
                GeometryCollection([]),
                None,
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().boundary
        self.check_sgpd_equals_gpd(df_result, expected)

        # Ensure M-dimension doesn't break things.
        s = GeoSeries(
            [
                wkt.loads("GEOMETRYCOLLECTION M (POINT M (1 2 3))"),
            ]
        )
        result = s.boundary
        expected = gpd.GeoSeries(
            [
                None,
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

    def test_centroid(self):
        s = sgpd.GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                LineString([(0, 0), (1, 1), (1, 0)]),
                Point(0, 0),
            ]
        )
        result = s.centroid
        expected = gpd.GeoSeries(
            [
                Point(0.3333333333333333, 0.6666666666666666),
                Point(0.7071067811865476, 0.5),
                Point(0, 0),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

    def test_concave_hull(self):
        s = GeoSeries(
            [
                MultiPoint([(0, 0), (1, 0), (0.5, 0.5), (1, 1), (0, 1)]),
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Point(0, 0),
                None,
            ]
        )
        expected = gpd.GeoSeries(
            [
                MultiPoint([(0, 0), (1, 0), (0.5, 0.5), (1, 1), (0, 1)]),
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Point(0, 0),
                None,
            ]
        ).concave_hull(ratio=0.5)
        result = s.concave_hull(ratio=0.5)
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().concave_hull(ratio=0.5)
        self.check_sgpd_equals_gpd(df_result, expected)

        # Use an asymmetric point set so that the Delaunay triangulation has no
        # ambiguous ties, and verify that allow_holes=True can produce a hole.
        points_around_hole = MultiPoint(
            [
                (11.1, 0.1),
                (6.2, 8.1),
                (0.1, 10.3),
                (-8.2, 7.1),
                (-10.1, -0.2),
                (-7.2, -8.3),
                (0.2, -11.1),
                (7.3, -7.2),
                (5.1, 0.2),
                (-0.1, 5.2),
                (-4.2, -0.9),
                (0.1, -4.1),
            ]
        )
        result_with_holes = GeoSeries([points_around_hole], crs=3857).concave_hull(
            ratio=0.9, allow_holes=True
        )
        expected_with_holes = gpd.GeoSeries(
            [
                Polygon(
                    [
                        (-8.2, 7.1),
                        (0.1, 10.3),
                        (6.2, 8.1),
                        (5.1, 0.2),
                        (11.1, 0.1),
                        (7.3, -7.2),
                        (0.2, -11.1),
                        (-7.2, -8.3),
                        (-10.1, -0.2),
                    ],
                    [[(-4.2, -0.9), (0.1, -4.1), (-0.1, 5.2)]],
                )
            ],
            crs=3857,
        )
        self.check_sgpd_equals_gpd(result_with_holes, expected_with_holes)
        assert result_with_holes.crs == expected_with_holes.crs

    def test_convex_hull(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                LineString([(0, 0), (2, 1)]),
                Point(0, 0),
                None,
            ]
        )

        result = s.convex_hull

        expected = gpd.GeoSeries(
            [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),  # polygon hull == itself
                LineString([(0, 0), (2, 1)]),  # NOT a polygon
                Point(0, 0),  # point stays point
                None,  # None stays None
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Check if GeoDataFrame works as well
        df_result = s.to_geoframe().convex_hull
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_delaunay_triangles(self):
        s = GeoSeries(
            [
                MultiPoint([(0, 0), (1, 0), (0.5, 1)]),
                MultiPoint([(0, 0), (1, 0), (1, 1), (0, 1)]),
            ]
        )
        # Sedona ST_DelaunayTriangles is element-wise (returns a GeometryCollection
        # per input), unlike geopandas which operates on all points at once.
        result = s.delaunay_triangles()
        result_gpd = result.to_geopandas()
        assert len(result_gpd) == 2
        # First input (3 points) should produce 1 triangle
        assert result_gpd.iloc[0].geom_type == "GeometryCollection"
        assert len(list(result_gpd.iloc[0].geoms)) == 1
        # Second input (4 points) should produce 2 triangles
        assert result_gpd.iloc[1].geom_type == "GeometryCollection"
        assert len(list(result_gpd.iloc[1].geoms)) == 2

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().delaunay_triangles()
        df_result_gpd = df_result.to_geopandas()
        assert len(df_result_gpd) == 2

    def test_constrained_delaunay_triangles(self):
        triangle = Polygon([(0, 0), (2, 0), (0, 2), (0, 0)])
        polygon_with_hole = Polygon(
            [(0, 0), (4, 0), (4, 4), (0, 4), (0, 0)],
            [[(1, 1), (3, 1), (3, 3), (1, 3), (1, 1)]],
        )
        multipolygon = MultiPolygon(
            [
                Polygon([(0, 0), (1, 0), (0, 1), (0, 0)]),
                Polygon([(3, 0), (4, 0), (3, 1), (3, 0)]),
            ]
        )
        geoms = [
            triangle,
            polygon_with_hole,
            multipolygon,
            Point(0, 0),
            Polygon(),
            None,
        ]
        index = pd.Index(
            ["triangle", "hole", "multi", "point", "empty", "null"],
            name="feature_id",
        )
        source = GeoSeries(geoms, index=index, crs="EPSG:3857")

        result = source.constrained_delaunay_triangles()

        actual = result.to_geopandas()
        assert actual.index.equals(index)
        for label, original in [
            ("triangle", triangle),
            ("hole", polygon_with_hole),
            ("multi", multipolygon),
        ]:
            triangles = actual.loc[label]
            assert triangles.geom_type == "GeometryCollection"
            assert shapely.union_all(list(triangles.geoms)).equals(original)

        assert actual.loc["point"].geom_type == "GeometryCollection"
        assert actual.loc["point"].is_empty
        assert actual.loc["empty"].geom_type == "GeometryCollection"
        assert actual.loc["empty"].is_empty
        assert actual.loc["null"] is None
        assert result.crs == source.crs

        srids = result._internal.spark_frame.select(
            stf.ST_SRID(result.spark.column).alias("srid")
        ).collect()
        assert {row.srid for row in srids if row.srid is not None} == {3857}

        # Check that GeoDataFrame works too.
        frame_result = source.to_geoframe().constrained_delaunay_triangles()
        self.check_sgpd_equals_gpd(frame_result, actual)
        assert frame_result.crs == source.crs

    def test_voronoi_polygons(self):
        s = GeoSeries(
            [
                MultiPoint([(0, 0), (1, 0), (0.5, 1)]),
            ]
        )
        # Sedona ST_VoronoiPolygons is element-wise, unlike geopandas
        result = s.voronoi_polygons()
        result_gpd = result.to_geopandas()
        assert len(result_gpd) == 1
        assert result_gpd.iloc[0].geom_type == "GeometryCollection"
        # 3 points should produce 3 Voronoi polygons
        assert len(list(result_gpd.iloc[0].geoms)) == 3

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().voronoi_polygons()
        df_result_gpd = df_result.to_geopandas()
        assert len(df_result_gpd) == 1

        # only_edges=True should raise
        with pytest.raises(NotImplementedError):
            s.voronoi_polygons(only_edges=True)

    def test_voronoi_polygons_extend_to(self):
        source_geometry = MultiPoint([(0, 0), (1, 0), (0.5, 1)])
        index = pd.MultiIndex.from_tuples([("group", 7)], names=["group", "feature_id"])
        source = GeoSeries([source_geometry], index=index, crs="EPSG:3857")

        default = source.voronoi_polygons()
        extended = source.voronoi_polygons(extend_to=box(-2, 0, 3, 3))
        contained = source.voronoi_polygons(extend_to=box(0.25, 0.25, 0.75, 0.75))
        nonrectangular = source.voronoi_polygons(
            extend_to=Polygon([(-2, 0), (3, 0), (0, 3)])
        )
        same_envelope = source.voronoi_polygons(extend_to=box(-2, 0, 3, 3))
        empty_extent = source.voronoi_polygons(extend_to=GeometryCollection())

        default_geometry = default.to_geopandas().iloc[0]
        extended_result = extended.to_geopandas()
        extended_geometry = extended_result.iloc[0]
        assert extended_result.index.equals(index)
        assert extended_geometry.bounds == pytest.approx((-2.0, -1.0, 3.0, 3.0))
        assert contained.to_geopandas().iloc[0].equals(default_geometry)
        assert (
            nonrectangular.to_geopandas()
            .iloc[0]
            .equals(same_envelope.to_geopandas().iloc[0])
        )
        assert empty_extent.to_geopandas().iloc[0].equals(default_geometry)
        assert extended.crs == source.crs

        srids = extended._internal.spark_frame.select(
            stf.ST_SRID(extended.spark.column).alias("srid")
        ).collect()
        assert [row.srid for row in srids] == [3857]

        frame_source = GeoSeries([source_geometry], crs="EPSG:3857")
        frame_result = frame_source.to_geoframe().voronoi_polygons(
            extend_to=box(-2, 0, 3, 3)
        )
        frame_expected = gpd.GeoSeries([extended_geometry], crs="EPSG:3857")
        self.check_sgpd_equals_gpd(frame_result, frame_expected)

        if hasattr(extended._internal.spark_frame, "_jdf"):
            plan = (
                extended._internal.spark_frame._jdf.queryExecution()
                .executedPlan()
                .toString()
            )
            assert "BatchEvalPython" not in plan
            assert "ArrowEvalPython" not in plan

        for invalid_extent in (
            [box(0, 0, 1, 1)],
            "POLYGON ((0 0, 1 0, 1 1, 0 0))",
            source,
            ps.Series([1]),
        ):
            with pytest.raises(TypeError, match="'extend_to' must be a geometry"):
                source.voronoi_polygons(extend_to=invalid_extent)

    def test_voronoi_polygons_extend_to_degenerate_inputs(self):
        source = GeoSeries.from_wkt(
            [
                None,
                "GEOMETRYCOLLECTION EMPTY",
                "POINT (0 0)",
                "MULTIPOINT ((0 0), (1 0))",
            ]
        )
        result = source.voronoi_polygons(extend_to=LineString([(-2, 0), (3, 0)]))
        actual = result.to_geopandas()

        assert actual.iloc[0] is None
        assert actual.iloc[1].is_empty
        assert actual.iloc[2].geom_type == "GeometryCollection"
        assert actual.iloc[2].bounds == pytest.approx((-2.0, 0.0, 3.0, 0.0))
        assert actual.iloc[3].geom_type == "GeometryCollection"
        assert actual.iloc[3].bounds == pytest.approx((-2.0, -1.0, 3.0, 1.0))

    def test_envelope(self):
        s = sgpd.GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                LineString([(0, 0), (1, 1), (1, 0)]),
                MultiPoint([(0, 0), (1, 1)]),
                Point(0, 0),
            ]
        )
        result = s.envelope
        expected = gpd.GeoSeries(
            [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]),
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]),
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]),
                Point(0, 0),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().envelope
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_minimum_rotated_rectangle(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                LineString([(0, 0), (2, 1)]),
                Point(0, 0),
                None,
            ]
        )
        expected = gpd.GeoSeries(
            [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                LineString([(0, 0), (2, 1)]),
                Point(0, 0),
                None,
            ]
        ).minimum_rotated_rectangle()
        result = s.minimum_rotated_rectangle()
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().minimum_rotated_rectangle()
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_exterior(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Polygon(
                    [(0, 0), (10, 0), (10, 10), (0, 10)],
                    [[(1, 1), (2, 1), (2, 2), (1, 2)]],
                ),
                None,
            ]
        )
        expected = gpd.GeoSeries(
            [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Polygon(
                    [(0, 0), (10, 0), (10, 10), (0, 10)],
                    [[(1, 1), (2, 1), (2, 2), (1, 2)]],
                ),
                None,
            ]
        ).exterior
        result = s.exterior
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().exterior
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_extract_unique_points(self):
        s = GeoSeries(
            [
                LineString([(0, 0), (1, 1), (0, 0)]),
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Point(0, 0),
                None,
            ]
        )
        expected = gpd.GeoSeries(
            [
                LineString([(0, 0), (1, 1), (0, 0)]),
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Point(0, 0),
                None,
            ]
        ).extract_unique_points()
        result = s.extract_unique_points()
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().extract_unique_points()
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_offset_curve(self):
        s = GeoSeries(
            [
                LineString([(0, 0), (0, 1), (1, 1)]),
                LineString([(0, 0), (10, 0)]),
            ]
        )

        result = s.offset_curve(1.0)
        expected = gpd.GeoSeries(
            [
                LineString([(0, 0), (0, 1), (1, 1)]),
                LineString([(0, 0), (10, 0)]),
            ]
        ).offset_curve(1.0)
        self.check_sgpd_equals_gpd(result, expected)

        # Negative distance (right side)
        result = s.offset_curve(-1.0)
        expected = gpd.GeoSeries(
            [
                LineString([(0, 0), (0, 1), (1, 1)]),
                LineString([(0, 0), (10, 0)]),
            ]
        ).offset_curve(-1.0)
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().offset_curve(1.0)
        expected = gpd.GeoSeries(
            [
                LineString([(0, 0), (0, 1), (1, 1)]),
                LineString([(0, 0), (10, 0)]),
            ]
        ).offset_curve(1.0)
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_interiors(self):
        polygon_with_holes = Polygon(
            [(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)],
            [
                [(1, 1), (2, 1), (1, 2), (1, 1)],
                [(3, 3), (4, 3), (4, 4), (3, 3)],
            ],
        )
        polygon_without_holes = Polygon([(10, 10), (10, 12), (12, 10), (10, 10)])
        index = pd.MultiIndex.from_tuples(
            [
                ("polygon", "holes"),
                ("polygon", "no-holes"),
                ("polygon", "empty"),
                ("point", "value"),
                ("point", "empty"),
                ("line", "empty"),
                ("multipolygon", "value"),
                ("collection", "value"),
                ("null", "value"),
            ],
            names=["geometry_type", "case"],
        )
        source = GeoSeries(
            [
                polygon_with_holes,
                polygon_without_holes,
                Polygon(),
                Point(0, 0),
                Point(),
                LineString(),
                MultiPolygon([polygon_without_holes]),
                GeometryCollection([polygon_with_holes]),
                None,
            ],
            index=index,
        )

        result = source.interiors

        assert isinstance(result, ps.Series)
        actual = result.to_pandas()
        pd.testing.assert_index_equal(actual.index, index)
        assert actual.dtype == object

        rings = actual.iloc[0]
        assert isinstance(rings, list)
        assert all(isinstance(ring, (LineString, LinearRing)) for ring in rings)
        assert [list(ring.coords) for ring in rings] == [
            [(1.0, 1.0), (2.0, 1.0), (1.0, 2.0), (1.0, 1.0)],
            [(3.0, 3.0), (4.0, 3.0), (4.0, 4.0), (3.0, 3.0)],
        ]
        assert actual.iloc[1] == []
        assert actual.iloc[2] == []
        assert all(value is None for value in actual.iloc[3:])

        # Check GeoDataFrame delegation separately; GeoSeries.to_geoframe()
        # does not currently accept a MultiIndex.
        delegated_index = pd.Index(
            ["holes", "no-holes", "point", "null"],
            name="feature_id",
        )
        delegated_source = GeoSeries(
            [
                polygon_with_holes,
                polygon_without_holes,
                Point(0, 0),
                None,
            ],
            index=delegated_index,
        )
        frame_result = delegated_source.to_geoframe().interiors
        assert isinstance(frame_result, ps.Series)
        frame_actual = frame_result.to_pandas()
        pd.testing.assert_index_equal(frame_actual.index, delegated_index)
        assert frame_actual.dtype == object
        assert [
            (
                None
                if value is None
                else [
                    tuple(tuple(coordinate) for coordinate in ring.coords)
                    for ring in value
                ]
            )
            for value in frame_actual
        ] == [
            [
                ((1.0, 1.0), (2.0, 1.0), (1.0, 2.0), (1.0, 1.0)),
                ((3.0, 3.0), (4.0, 3.0), (4.0, 4.0), (3.0, 3.0)),
            ],
            [],
            None,
            None,
        ]

    def test_remove_repeated_points(self):
        s = GeoSeries(
            [
                LineString([(0, 0), (0, 0), (1, 1), (1, 1), (2, 2)]),
                Polygon([(0, 0), (1, 0), (1, 0), (1, 1), (0, 1)]),
                Point(0, 0),
                None,
            ]
        )
        expected = gpd.GeoSeries(
            [
                LineString([(0, 0), (0, 0), (1, 1), (1, 1), (2, 2)]),
                Polygon([(0, 0), (1, 0), (1, 0), (1, 1), (0, 1)]),
                Point(0, 0),
                None,
            ]
        ).remove_repeated_points()
        result = s.remove_repeated_points()
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().remove_repeated_points()
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_set_precision(self):
        pass

    def test_representative_point(self):
        geoms = [
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            LineString([(0, 0), (1, 1), (1, 0)]),
            Point(0, 0),
            None,
        ]
        s = GeoSeries(geoms)
        expected = gpd.GeoSeries(geoms).representative_point()

        result = s.representative_point()
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().representative_point()
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_minimum_bounding_circle(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                LineString([(0, 0), (2, 0)]),
                Point(0, 0),
                None,
            ]
        )

        expected = gpd.GeoSeries(
            [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                LineString([(0, 0), (2, 0)]),
                Point(0, 0),
                None,
            ]
        ).minimum_bounding_circle()

        result = s.minimum_bounding_circle()
        self.check_sgpd_equals_gpd(result, expected)

        gdf = s.to_geoframe()
        df_result = gdf.minimum_bounding_circle()
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_maximum_inscribed_circle(self):
        geoms = [
            Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]),
            Polygon([(0, 0), (0.5, -1), (1, 0), (1, 1), (-0.5, 0.5)]),
            MultiPolygon(
                [
                    Polygon([(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]),
                    Polygon([(10, 0), (10, 1), (11, 1), (11, 0), (10, 0)]),
                ]
            ),
            None,
        ]
        index = pd.Index(["first", "first", "multi", "null"], name="feature_id")
        source = GeoSeries(geoms, index=index, crs="EPSG:3857")
        # Keep this direct test runnable with GeoPandas < 1.1. The version-gated
        # parity test compares these results with GeoPandas dynamically.
        expected = gpd.GeoSeries(
            [
                LineString([(0.70703125, 0.29296875), (0.5, 0.5)]),
                LineString([(0.466796875, 0.259765625), (1, 0.259765625)]),
                LineString([(1, 1), (0, 1)]),
                None,
            ],
            index=index,
            crs="EPSG:3857",
        )

        result = source.maximum_inscribed_circle()
        self.check_sgpd_equals_gpd(result, expected)
        assert result.name is None
        assert result.crs == source.crs
        srids = result._internal.spark_frame.select(
            stf.ST_SRID(result.spark.column).alias("srid")
        ).collect()
        assert {row.srid for row in srids if row.srid is not None} == {3857}

        frame = GeoDataFrame(
            gpd.GeoDataFrame(
                {
                    "geometry": geoms,
                    "tolerance": [0.0, 2.0, 0.5, np.nan],
                    "label": ["a", "b", "c", "d"],
                },
                index=index,
                crs="EPSG:3857",
            )
        )
        row_wise = frame.geometry.maximum_inscribed_circle(tolerance=frame["tolerance"])
        row_wise_expected = gpd.GeoSeries(
            [
                LineString([(0.70703125, 0.29296875), (0.5, 0.5)]),
                LineString([(0.375, 0.25), (0, 0)]),
                LineString([(1, 1), (0, 1)]),
                None,
            ],
            index=index,
            crs="EPSG:3857",
        )
        self.check_sgpd_equals_gpd(row_wise, row_wise_expected)

        local_tolerances = [0.0, 2.0, 0.5, np.nan]
        for local_tolerance in (
            local_tolerances,
            np.asarray(local_tolerances),
            pd.Series(local_tolerances, index=index),
        ):
            local_result = source.maximum_inscribed_circle(tolerance=local_tolerance)
            self.check_sgpd_equals_gpd(local_result, row_wise_expected)

        self.check_sgpd_equals_gpd(
            source.maximum_inscribed_circle(tolerance=0),
            expected,
        )
        self.check_sgpd_equals_gpd(
            source.maximum_inscribed_circle(tolerance=np.asarray(0)),
            expected,
        )
        self.check_sgpd_equals_gpd(
            source.maximum_inscribed_circle(tolerance=[0]),
            expected,
        )
        assert (
            len(
                GeoSeries([], crs="EPSG:3857").maximum_inscribed_circle(
                    tolerance=np.array([0])
                )
            )
            == 0
        )
        self.check_sgpd_equals_gpd(
            frame.maximum_inscribed_circle(tolerance=2.0),
            gpd.GeoSeries(
                [
                    LineString([(0.75, 0.5), (0.625, 0.625)]),
                    LineString([(0.375, 0.25), (0, 0)]),
                    LineString([(1, 1), (0, 1)]),
                    None,
                ],
                index=index,
                crs="EPSG:3857",
            ),
        )

        spark_frame = row_wise._internal.spark_frame
        if hasattr(spark_frame, "_jdf"):
            optimized_plan = (
                spark_frame._jdf.queryExecution().optimizedPlan().toString()
            )
            assert optimized_plan.lower().count("st_maximuminscribedcircle") == 1
            assert "BatchEvalPython" not in optimized_plan
            assert "ArrowEvalPython" not in optimized_plan
            assert "PythonUDF" not in optimized_plan

        nan_result = source.maximum_inscribed_circle(tolerance=np.nan).to_geopandas()
        assert nan_result.isna().all()

        degenerate = GeoSeries(
            [Polygon([(0, 0), (0, 0), (0, 0), (0, 0)])],
            crs="EPSG:3857",
        )
        self.check_sgpd_equals_gpd(
            degenerate.maximum_inscribed_circle(),
            gpd.GeoSeries(
                [LineString([(0, 0), (0, 0)])],
                crs="EPSG:3857",
            ),
        )
        self.check_sgpd_equals_gpd(
            degenerate.maximum_inscribed_circle(tolerance=2.0),
            gpd.GeoSeries(
                [LineString([(0, 0), (0, 0)])],
                crs="EPSG:3857",
            ),
        )

        with pytest.raises(ValueError, match="'tolerance' should be positive"):
            source.maximum_inscribed_circle(tolerance=-1)
        with pytest.raises(ValueError, match="'tolerance' should be positive"):
            source.maximum_inscribed_circle(tolerance=np.array(-1.0))
        with pytest.raises(Exception, match="'tolerance' should be positive"):
            source.maximum_inscribed_circle(
                tolerance=[-1.0] * len(source)
            ).to_geopandas()
        with pytest.raises(Exception, match="'tolerance' should be positive"):
            frame.geometry.maximum_inscribed_circle(
                tolerance=frame["tolerance"] - 3.0
            ).to_geopandas()
        with pytest.raises(ValueError, match="must share the same frame"):
            source.maximum_inscribed_circle(tolerance=ps.Series([0.1] * len(source)))
        with pytest.raises(TypeError, match="numeric scalar"):
            frame.geometry.maximum_inscribed_circle(tolerance=frame["label"])
        with pytest.raises(TypeError, match="numeric scalar"):
            source.maximum_inscribed_circle(tolerance=["0.1"] * len(source))
        with pytest.raises(TypeError, match="numeric scalar"):
            source.maximum_inscribed_circle(tolerance=Decimal("0.1"))
        with pytest.raises(TypeError, match="numeric scalar"):
            source.maximum_inscribed_circle(tolerance=[Decimal("0.1")] * len(source))
        for mismatched_tolerance in (
            [0.1] * (len(source) - 1),
            [0.1] * (len(source) + 1),
        ):
            with pytest.raises(Exception, match="Length of tolerance"):
                source.maximum_inscribed_circle(
                    tolerance=mismatched_tolerance
                ).to_geopandas()
        with pytest.raises(Exception, match="Index of the Series"):
            source.maximum_inscribed_circle(
                tolerance=pd.Series(
                    local_tolerances,
                    index=pd.Index(
                        ["first", "multi", "first", "null"],
                        name="feature_id",
                    ),
                )
            ).to_geopandas()

        with pytest.raises(
            Exception,
            match="Input geometry must be a Polygon or MultiPolygon",
        ):
            GeoSeries([Point(0, 0)]).maximum_inscribed_circle().to_geopandas()
        with pytest.raises(
            Exception,
            match="Empty input geometry is not supported",
        ):
            GeoSeries([Polygon()]).maximum_inscribed_circle().to_geopandas()

    def test_maximum_inscribed_circle_local_tolerance_preserves_order(self):
        adaptive_enabled = self.spark.conf.get("spark.sql.adaptive.enabled")
        shuffle_partitions = self.spark.conf.get("spark.sql.shuffle.partitions")
        try:
            self.spark.conf.set("spark.sql.adaptive.enabled", "false")
            self.spark.conf.set("spark.sql.shuffle.partitions", "8")
            expected_index = pd.Index(
                [f"feature-{position:03d}" for position in range(100)],
                name="feature_id",
            )
            source = GeoSeries(
                [Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])] * 100,
                index=expected_index,
            )

            actual = source.maximum_inscribed_circle(
                tolerance=[2.0] * len(source)
            ).to_geopandas()

            pd.testing.assert_index_equal(actual.index, expected_index)
        finally:
            self.spark.conf.set("spark.sql.adaptive.enabled", adaptive_enabled)
            self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

    def test_minimum_bounding_radius(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1), (0, 0)]),
                LineString([(0, 0), (1, 1), (1, 0)]),
                Point(0, 0),
            ]
        )

        expected = pd.Series(
            [
                0.707107,  # radius for the square
                0.707107,  # radius for the line
                0.000000,  # radius for the point
            ]
        )

        result = s.minimum_bounding_radius()
        self.check_pd_series_equal(result, expected)

        gdf = s.to_geoframe()
        df_result = gdf.minimum_bounding_radius()
        self.check_pd_series_equal(df_result, expected)

    def test_minimum_clearance(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Polygon([(0, 0), (0.5, 0), (0.5, 0.5), (0, 0.5)]),
                MultiPoint([(1, 1), (1, 1)]),
            ]
        )
        expected = pd.Series([1.0, 0.5, float("inf")])
        result = s.minimum_clearance()
        self.check_pd_series_equal(result, expected)

        gdf = s.to_geoframe()
        df_result = gdf.minimum_clearance()
        self.check_pd_series_equal(df_result, expected)

    def test_minimum_clearance_line(self):
        geoms = [
            Polygon([(0, 0), (1, 1), (0, 1), (0, 0)]),
            LineString([(0, 0), (1, 1), (3, 2)]),
            MultiPoint([(0, 0), (3, 4)]),
            MultiPoint([(1, 1), (1, 1)]),
            Point(0, 0),
            Point(),
            LineString(),
            Polygon(),
            None,
        ]
        index = pd.Index(
            [
                "polygon",
                "line",
                "multipoint",
                "duplicate",
                "point",
                "empty-point",
                "empty-line",
                "empty-polygon",
                "null",
            ],
            name="feature_id",
        )
        source = GeoSeries(geoms, index=index, crs="EPSG:3857")
        expected = gpd.GeoSeries(
            [
                LineString([(0, 1), (0.5, 0.5)]),
                LineString([(0, 0), (1, 1)]),
                LineString([(3, 4), (0, 0)]),
                LineString(),
                LineString(),
                LineString(),
                LineString(),
                LineString(),
                None,
            ],
            index=index,
            crs="EPSG:3857",
        )

        result = source.minimum_clearance_line()

        self.check_sgpd_equals_gpd(result, expected)
        assert result.crs == source.crs
        actual = result.to_geopandas()
        for label in [
            "duplicate",
            "point",
            "empty-point",
            "empty-line",
            "empty-polygon",
        ]:
            assert actual.loc[label].geom_type == "LineString"
            assert actual.loc[label].is_empty
        assert actual.loc["null"] is None

        srids = result._internal.spark_frame.select(
            stf.ST_SRID(result.spark.column).alias("srid")
        ).collect()
        assert {row.srid for row in srids if row.srid is not None} == {3857}

        # Check that GeoDataFrame works too.
        frame_result = source.to_geoframe().minimum_clearance_line()
        self.check_sgpd_equals_gpd(frame_result, expected)
        assert frame_result.crs == source.crs

    def test_normalize(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                LineString([(0, 0), (1, 1)]),
                Point(0, 0),
                None,
            ]
        )
        result = s.normalize()
        expected = gpd.GeoSeries(
            [
                shapely.normalize(Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])),
                shapely.normalize(LineString([(0, 0), (1, 1)])),
                shapely.normalize(Point(0, 0)),
                None,
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().normalize()
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_orient_polygons(self):
        clockwise_polygon = Polygon(
            [(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)],
            [[(1, 1), (4, 1), (4, 4), (1, 4), (1, 1)]],
        )
        second_polygon = Polygon([(10, 0), (10, 2), (12, 2), (12, 0), (10, 0)])
        multipolygon = MultiPolygon([clockwise_polygon, second_polygon])
        nested_collection = GeometryCollection(
            [
                Point(20, 20),
                GeometryCollection(
                    [
                        clockwise_polygon,
                        MultiPolygon([second_polygon]),
                    ]
                ),
            ]
        )
        geoms = [
            clockwise_polygon,
            multipolygon,
            nested_collection,
            Point(1, 1),
            LineString([(0, 0), (1, 1)]),
            Point(),
            LineString(),
            Polygon(),
            MultiPolygon(),
            GeometryCollection(),
            None,
        ]
        index = pd.Index(
            [
                "polygon",
                "multipolygon",
                "nested",
                "point",
                "line",
                "empty-point",
                "empty-line",
                "empty-polygon",
                "empty-multipolygon",
                "empty-collection",
                "null",
            ],
            name="feature_id",
        )
        source = GeoSeries(geoms, index=index, crs="EPSG:3857")
        expected = gpd.GeoSeries(geoms, index=index, crs="EPSG:3857")

        def assert_oriented(geometry, exterior_cw):
            if geometry is None or geometry.is_empty:
                return
            if isinstance(geometry, Polygon):
                assert bool(geometry.exterior.is_ccw) is not exterior_cw
                assert all(
                    bool(ring.is_ccw) is exterior_cw for ring in geometry.interiors
                )
            elif isinstance(geometry, (MultiPolygon, GeometryCollection)):
                for part in geometry.geoms:
                    assert_oriented(part, exterior_cw)

        for exterior_cw in (False, True):
            result = source.orient_polygons(exterior_cw=exterior_cw)

            self.check_sgpd_equals_gpd(result, expected)
            assert result.crs == source.crs
            actual = result.to_geopandas().sort_index()
            for geometry in actual:
                assert_oriented(geometry, exterior_cw)

            nested = actual.loc["nested"]
            assert isinstance(nested, GeometryCollection)
            assert isinstance(nested.geoms[1], GeometryCollection)
            assert isinstance(nested.geoms[1].geoms[1], MultiPolygon)

            for label, geometry_type in [
                ("empty-point", "Point"),
                ("empty-line", "LineString"),
                ("empty-polygon", "Polygon"),
                ("empty-multipolygon", "MultiPolygon"),
                ("empty-collection", "GeometryCollection"),
            ]:
                assert actual.loc[label].is_empty
                assert actual.loc[label].geom_type == geometry_type
            assert actual.loc["null"] is None

            srids = result._internal.spark_frame.select(
                stf.ST_SRID(result.spark.column).alias("srid")
            ).collect()
            assert {row.srid for row in srids if row.srid is not None} == {3857}

            # Check that GeoDataFrame works too.
            frame_result = source.to_geoframe().orient_polygons(exterior_cw=exterior_cw)
            self.check_sgpd_equals_gpd(frame_result, expected)
            assert frame_result.crs == source.crs
            frame_actual = frame_result.to_geopandas().sort_index()
            for geometry in frame_actual:
                assert_oriented(geometry, exterior_cw)

    def test_make_valid(self):
        s = sgpd.GeoSeries(
            [
                Polygon([(0, 0), (0, 2), (1, 1), (2, 2), (2, 0), (1, 1), (0, 0)]),
                Polygon([(0, 2), (0, 1), (2, 0), (0, 0), (0, 2)]),
                LineString([(0, 0), (1, 1), (1, 0)]),
            ],
        )
        result = s.make_valid(method="structure")

        expected = gpd.GeoSeries(
            [
                MultiPolygon(
                    [
                        Polygon([(1, 1), (0, 0), (0, 2), (1, 1)]),
                        Polygon([(2, 0), (1, 1), (2, 2), (2, 0)]),
                    ]
                ),
                Polygon([(0, 1), (2, 0), (0, 0), (0, 1)]),
                LineString([(0, 0), (1, 1), (1, 0)]),
            ]
        )

        self.check_sgpd_equals_gpd(result, expected)

        result = s.make_valid(method="structure", keep_collapsed=False)
        expected = gpd.GeoSeries(
            [
                MultiPolygon(
                    [
                        Polygon([(1, 1), (0, 0), (0, 2), (1, 1)]),
                        Polygon([(2, 0), (1, 1), (2, 2), (2, 0)]),
                    ]
                ),
                Polygon([(0, 1), (2, 0), (0, 0), (0, 1)]),
                LineString([(0, 0), (1, 1), (1, 0)]),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        s = GeoSeries([Polygon([(0, 0), (1, 1), (1, 2), (1, 1), (0, 0)])])

        result = s.make_valid(method="structure", keep_collapsed=True)
        expected = gpd.GeoSeries([LineString([(0, 0), (1, 1), (1, 2), (1, 1), (0, 0)])])
        self.check_sgpd_equals_gpd(result, expected)

        result = s.make_valid(method="structure", keep_collapsed=False)
        expected = gpd.GeoSeries([Polygon()])
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().make_valid(method="structure", keep_collapsed=False)
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_reverse(self):
        s = GeoSeries(
            [
                LineString([(0, 0), (1, 1), (2, 2)]),
                LineString([(0, 0), (1, 0), (1, 1)]),
                Point(0, 0),
                None,
            ]
        )
        result = s.reverse()
        expected = gpd.GeoSeries(
            [
                LineString([(2, 2), (1, 1), (0, 0)]),
                LineString([(1, 1), (1, 0), (0, 0)]),
                Point(0, 0),
                None,
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().reverse()
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_sample_points(self):
        polygon = box(0, 0, 2, 2)
        multipolygon = MultiPolygon([box(4, 0, 5, 1), box(7, 0, 9, 2)])
        line = LineString([(0, 0), (2, 0), (2, 2)])
        multiline = MultiLineString([[(0, 4), (1, 4)], [(3, 4), (3, 7)]])
        geometries = [
            polygon,
            multipolygon,
            line,
            multiline,
            Point(1, 1),
            MultiPoint([(0, 0), (1, 1)]),
            GeometryCollection([Point(0, 0)]),
            Polygon(),
            LineString(),
            None,
        ]
        index = pd.Index(
            [
                "area",
                "area",
                "line",
                "line",
                "point",
                "points",
                "gc",
                "ep",
                "el",
                "null",
            ],
            name="feature_id",
        )
        source = GeoSeries(geometries, index=index, crs="EPSG:3857")

        result = source.sample_points(4, rng=0)
        actual = result.to_geopandas()
        repeated = source.sample_points(4, rng=0).to_geopandas()

        assert isinstance(result, GeoSeries)
        assert result.name == "sampled_points"
        assert result.crs == source.crs
        assert actual.index.equals(index)
        assert [geometry.wkb for geometry in actual] == [
            geometry.wkb for geometry in repeated
        ]
        for source_geometry, sampled in zip(geometries[:4], actual.iloc[:4]):
            assert sampled.geom_type == "MultiPoint"
            assert len(sampled.geoms) == 4
            assert all(source_geometry.covers(point) for point in sampled.geoms)
        for sampled in actual.iloc[4:]:
            assert sampled.geom_type == "MultiPoint"
            assert sampled.is_empty

        srids = result._internal.spark_frame.select(
            stf.ST_SRID(result.spark.column).alias("srid")
        ).collect()
        assert {row.srid for row in srids} == {3857}

        if hasattr(result._internal.spark_frame, "_jdf"):
            plan = (
                result._internal.spark_frame._jdf.queryExecution()
                .executedPlan()
                .toString()
            )
            assert "BatchEvalPython" not in plan
            assert "ArrowEvalPython" not in plan
            assert "PythonUDF" not in plan

        frame_result = source.to_geoframe().sample_points(4, rng=0)
        assert isinstance(frame_result, GeoSeries)
        assert frame_result.name == "sampled_points"
        assert frame_result.crs == source.crs

        zero = GeoSeries([polygon, line, Point(0, 0)]).sample_points(0, rng=1)
        zero_types = [geometry.geom_type for geometry in zero.to_geopandas()]
        assert zero_types == ["MultiPoint", "MultiPoint", "MultiPoint"]

        one = GeoSeries([polygon, line, Point(0, 0)]).sample_points(1, rng=1)
        one_types = [geometry.geom_type for geometry in one.to_geopandas()]
        assert one_types == ["MultiPoint", "MultiPoint", "MultiPoint"]

        degenerate_lines = GeoSeries(
            [
                LineString([(0, 0), (0, 0)]),
                MultiLineString([[(1, 1), (1, 1)], [(1, 1), (1, 1)]]),
            ]
        ).sample_points(4, rng=1)
        degenerate_actual = degenerate_lines.to_geopandas()
        assert [geometry.geom_type for geometry in degenerate_actual] == [
            "MultiPoint",
            "MultiPoint",
        ]
        assert all(
            len(geometry.geoms) == 4
            and all(point.equals(Point(i, i)) for point in geometry.geoms)
            for i, geometry in enumerate(degenerate_actual)
        )

        multi_index = pd.MultiIndex.from_tuples(
            [("a", 1), ("a", 2), ("b", 1), ("b", 2)],
            names=["group", "position"],
        )
        distributed_source = GeoSeries(
            [polygon, multipolygon, line, multiline],
            index=multi_index,
            crs="EPSG:3857",
        )
        distributed_result = distributed_source.sample_points(
            ps.Series([1, 2, 3, 4]), rng=7
        ).to_geopandas()
        assert distributed_result.index.equals(multi_index)
        assert [
            1 if geometry.geom_type == "Point" else len(geometry.geoms)
            for geometry in distributed_result
        ] == [1, 2, 3, 4]

        extra_sizes = distributed_source.sample_points(
            [1, 2, 3, 4, 99], rng=7
        ).to_geopandas()
        assert [
            1 if geometry.geom_type == "Point" else len(geometry.geoms)
            for geometry in extra_sizes
        ] == [1, 2, 3, 4]

        same_anchor_index = pd.Index(
            ["duplicate", "duplicate", "other", "other"],
            name="feature_id",
        )
        same_anchor_frame = GeoDataFrame(
            gpd.GeoDataFrame(
                {
                    "geometry": [polygon, multipolygon, line, multiline],
                    "size": [1, 2, 3, 4],
                },
                index=same_anchor_index,
                crs="EPSG:3857",
            )
        )
        same_anchor_result = same_anchor_frame.geometry.sample_points(
            same_anchor_frame["size"], rng=7
        )
        same_anchor_actual = same_anchor_result.to_geopandas()
        assert same_anchor_actual.index.equals(same_anchor_index)
        assert [
            1 if geometry.geom_type == "Point" else len(geometry.geoms)
            for geometry in same_anchor_actual
        ] == [1, 2, 3, 4]
        if hasattr(same_anchor_result._internal.spark_frame, "_jdf"):
            plan = (
                same_anchor_result._internal.spark_frame._jdf.queryExecution()
                .optimizedPlan()
                .toString()
            )
            assert "Join" not in plan
            assert "AttachDistributedSequence" not in plan

        # Like GeoPandas, an integer seed restarts the same random stream for
        # every row, so identical geometries receive identical samples.
        identical = GeoSeries(
            [line, line],
            index=pd.Index(["duplicate", "duplicate"], name="feature_id"),
        ).sample_points(4, rng=7)
        identical_actual = identical.to_geopandas()
        assert identical_actual.iloc[0].wkb == identical_actual.iloc[1].wkb

        for sizes in ([], [1, 2, 3]):
            empty_result = GeoSeries([], crs="EPSG:3857").sample_points(sizes, rng=7)
            assert len(empty_result) == 0
            assert empty_result.name == "sampled_points"
            assert empty_result.crs == "EPSG:3857"

        empty_float_sizes = GeoSeries([], crs="EPSG:3857").sample_points(
            ps.Series([], dtype=float), rng=7
        )
        assert len(empty_float_sizes) == 0
        assert empty_float_sizes.name == "sampled_points"
        assert empty_float_sizes.crs == "EPSG:3857"

        with pytest.raises(TypeError):
            source.sample_points(True)
        with pytest.raises(TypeError):
            source.sample_points(1.5)
        with pytest.raises(ValueError):
            source.sample_points(-1)
        with pytest.raises(NotImplementedError):
            source.sample_points(1, method="cluster_poisson")
        with pytest.raises(Exception, match="sample size values must be integers"):
            source.sample_points(ps.Series([1.0] * len(source))).to_geopandas()
        for short_sizes in ([], [1]):
            with pytest.raises(Exception, match="Length of sample sizes"):
                source.sample_points(short_sizes, rng=1).to_geopandas()
        with pytest.warns(FutureWarning, match="'seed' keyword is deprecated"):
            source.sample_points(1, seed=1)

    def test_sample_points_stateful_rng_is_partition_stable(self):
        line = LineString([(0, 0), (2, 0), (2, 2)])
        identical_source = GeoSeries(
            [line, line, line, line],
            index=pd.Index(["duplicate"] * 4, name="feature_id"),
        )
        test_position_col = "__sample_points_test_position__"
        positioned_frame = InternalFrame.attach_distributed_sequence_column(
            identical_source._internal.spark_frame.orderBy(NATURAL_ORDER_COLUMN_NAME),
            test_position_col,
        )
        ordered_rows = positioned_frame.select(
            F.col(test_position_col).alias("position"),
            scol_for(
                positioned_frame,
                identical_source._internal.index_spark_column_names[0],
            ).alias("feature_id"),
            scol_for(
                positioned_frame,
                identical_source._internal.data_spark_column_names[0],
            ).alias("geometry"),
        )

        # These encodings have the same row order but mimic natural-order IDs
        # generated with one partition and with a boundary before position 2.
        natural_order_encodings = [
            F.col("position"),
            F.when(F.col("position") < 2, F.col("position")).otherwise(
                F.lit(1 << 33) + F.col("position") - F.lit(2)
            ),
        ]

        def source_with_natural_order(encoding):
            spark_frame = ordered_rows.select(
                F.col("feature_id"),
                F.col("geometry"),
                encoding.cast("long").alias(NATURAL_ORDER_COLUMN_NAME),
            )
            return GeoSeries(spark_frame.pandas_api(index_col="feature_id")["geometry"])

        for rng_factory in (
            lambda: np.random.default_rng(0),
            lambda: np.random.PCG64(0),
        ):
            stateful_results = [
                source_with_natural_order(encoding).sample_points(4, rng=rng_factory())
                for encoding in natural_order_encodings
            ]
            assert (
                stateful_results[0]
                .to_geopandas()
                .index.equals(pd.Index(["duplicate"] * 4, name="feature_id"))
            )
            ordered_wkb = [
                [
                    row.geometry.wkb
                    for row in result._internal.spark_frame.select(
                        result.spark.column.alias("geometry"),
                        scol_for(
                            result._internal.spark_frame,
                            NATURAL_ORDER_COLUMN_NAME,
                        ).alias("natural_order"),
                    )
                    .orderBy("natural_order")
                    .collect()
                ]
                for result in stateful_results
            ]
            assert ordered_wkb[0] == ordered_wkb[1]
            assert len(set(ordered_wkb[0])) == len(ordered_wkb[0])

    def test_segmentize(self):
        s = GeoSeries(
            [
                LineString([(0, 0), (0, 10)]),
                Polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]),
            ],
        )
        result = s.segmentize(5)
        expected = gpd.GeoSeries(
            [
                LineString([(0, 0), (0, 5), (0, 10)]),
                Polygon(
                    [
                        (0, 0),
                        (5, 0),
                        (10, 0),
                        (10, 5),
                        (10, 10),
                        (5, 10),
                        (0, 10),
                        (0, 5),
                        (0, 0),
                    ]
                ),
            ],
        )
        self.check_sgpd_equals_gpd(result, expected)

        df_result = s.to_geoframe().segmentize(5)
        self.check_sgpd_equals_gpd(df_result, expected)

        # Test array-like input
        result = s.segmentize(ps.Series([5, 10]))
        expected = gpd.GeoSeries(
            [
                LineString([(0, 0), (0, 5), (0, 10)]),
                Polygon(
                    [
                        (0, 0),
                        (10, 0),
                        (10, 10),
                        (0, 10),
                        (0, 0),
                    ]
                ),
            ],
        )
        self.check_sgpd_equals_gpd(result, expected)

    def test_affine_transform_preserves_metadata_and_delegates(self):
        geoms = [
            Point(1, 2),
            LineString([(0, 0), (2, 1)]),
            Polygon([(0, 0), (2, 0), (1, 1), (0, 0)]),
            MultiPoint([(0, 0), (1, 2)]),
            Polygon(),
            None,
        ]
        index = pd.Index(
            ["point", "line", "polygon", "multipoint", "empty", "null"],
            name="feature_id",
        )
        matrix = [2, 1, -1, 3, 4, -5]
        source = GeoSeries(geoms, index=index, crs="EPSG:3857")
        expected = gpd.GeoSeries(geoms, index=index, crs="EPSG:3857").affine_transform(
            matrix
        )

        result = source.affine_transform(matrix)

        self.check_sgpd_equals_gpd(result, expected)
        assert result.crs == source.crs == expected.crs
        actual = result.to_geopandas()
        assert actual.loc["empty"].is_empty
        assert actual.loc["empty"].geom_type == "Polygon"
        assert actual.loc["null"] is None

        srids = result._internal.spark_frame.select(
            stf.ST_SRID(result.spark.column).alias("srid")
        ).collect()
        assert {row.srid for row in srids if row.srid is not None} == {3857}

        # GeoDataFrame inherits the base geometry-column delegation.
        frame_result = source.to_geoframe().affine_transform(matrix)
        assert isinstance(frame_result, GeoSeries)
        self.check_sgpd_equals_gpd(frame_result, expected)
        assert frame_result.crs == source.crs

    def test_affine_transform_3d_coefficient_order(self):
        source = GeoSeries([wkt.loads("POINT Z (1 2 3)")], crs="EPSG:4326")
        result = source.affine_transform(range(1, 13))

        point = result.to_geopandas().iloc[0]
        assert tuple(point.coords[0]) == pytest.approx((24.0, 43.0, 62.0))
        srid = result._internal.spark_frame.select(
            stf.ST_SRID(result.spark.column).alias("srid")
        ).first()
        assert srid.srid == 4326
        assert result.crs == source.crs

    def test_affine_transform_dimension_handling(self):
        z_geoms = [
            Point(1, 2, 3),
            LineString([(0, 0, 4), (2, 1, 5)]),
            Polygon([(0, 0, 1), (2, 0, 2), (1, 1, 3), (0, 0, 1)]),
        ]
        matrix_2d = [2, 0, 0, 3, 4, -5]
        result_2d_on_z = GeoSeries(z_geoms).affine_transform(matrix_2d)
        expected_2d_on_z = gpd.GeoSeries(z_geoms).affine_transform(matrix_2d)
        self.check_sgpd_equals_gpd(result_2d_on_z, expected_2d_on_z)

        actual_2d_on_z = result_2d_on_z.to_geopandas()
        assert all(geom.has_z for geom in actual_2d_on_z)
        assert actual_2d_on_z.iloc[0].z == pytest.approx(3.0)
        assert [coord[2] for coord in actual_2d_on_z.iloc[1].coords] == [4.0, 5.0]

        xy_geoms = [
            Point(1, 2),
            LineString([(0, 0), (2, 1)]),
            Polygon([(0, 0), (2, 0), (1, 1), (0, 0)]),
        ]
        matrix_3d = [
            1.0,
            0.5,
            0.25,
            -0.5,
            2.0,
            0.75,
            0.1,
            -0.2,
            1.5,
            3.0,
            -4.0,
            5.0,
        ]
        result_3d_on_2d = GeoSeries(xy_geoms).affine_transform(matrix_3d)
        expected_3d_on_2d = gpd.GeoSeries(xy_geoms).affine_transform(matrix_3d)
        self.check_sgpd_equals_gpd(result_3d_on_2d, expected_3d_on_2d)
        assert not any(geom.has_z for geom in result_3d_on_2d.to_geopandas())

        special_geoms = [Polygon(), None]
        result_3d_special = GeoSeries(special_geoms).affine_transform(matrix_3d)
        expected_3d_special = gpd.GeoSeries(special_geoms).affine_transform(matrix_3d)
        self.check_sgpd_equals_gpd(result_3d_special, expected_3d_special)
        actual_3d_special = result_3d_special.to_geopandas()
        assert actual_3d_special.iloc[0].is_empty
        assert actual_3d_special.iloc[0].geom_type == "Polygon"
        assert actual_3d_special.iloc[1] is None

    def test_affine_transform_validates_matrix(self):
        source = GeoSeries([Point(1, 2)])

        class OversizedSequence:
            def __len__(self):
                return 10**9

            def __iter__(self):
                raise AssertionError("invalid-length matrices must not be iterated")

        with pytest.raises(ValueError, match="either 6 or 12 coefficients"):
            source.affine_transform(OversizedSequence())

        class InconsistentSequence:
            def __len__(self):
                return 6

            def __iter__(self):
                return iter([1] * 7)

        with pytest.raises(ValueError, match="either 6 or 12 coefficients"):
            source.affine_transform(InconsistentSequence())

        for matrix in ([1] * 5, [1] * 7, [1] * 11, [1] * 13):
            with pytest.raises(ValueError):
                source.affine_transform(matrix)

        for coefficient in ("not-numeric", "1", None, np.array([1])):
            matrix = [1, 0, 0, 1, 0, coefficient]
            with pytest.raises(TypeError, match="only numeric coefficients"):
                source.affine_transform(matrix)

        for matrix in ("123456", 123456, {1, 2, 3, 4, 5, 6}):
            with pytest.raises(TypeError, match="local ordered sequence"):
                source.affine_transform(matrix)

        with pytest.raises(TypeError, match="local ordered sequence"):
            source.affine_transform(ps.Series([1, 0, 0, 1, 0, 0]))

    def test_transform(self):
        pass

    def test_rotate(self):
        geoms = [
            Point(1, 1),
            LineString([(1, -1), (1, 0)]),
            Polygon([(3, -1), (4, 0), (3, 1)]),
            None,
        ]
        s = GeoSeries(geoms)

        # Test default
        result = s.rotate(90)
        expected = gpd.GeoSeries(
            [
                Point(1.0, 1.0),
                LineString([(1.5, -0.5), (0.5, -0.5)]),
                Polygon([(4.5, -0.5), (3.5, 0.5), (2.5, -0.5), (4.5, -0.5)]),
                None,
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Test with explicit origin tuple (90 degrees around 0,0)
        result = s.rotate(90, origin=(0, 0))
        expected = gpd.GeoSeries(
            [
                Point(-1.0, 1.0),
                LineString([(1.0, 1.0), (0.0, 1.0)]),
                Polygon([(1.0, 3.0), (0.0, 4.0), (-1.0, 3.0), (1.0, 3.0)]),
                None,
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Test use_radians
        import math

        result = s.rotate(math.pi / 2, origin=(0, 0), use_radians=True)
        self.check_sgpd_equals_gpd(result, expected)

        # Test with a Point object as the origin
        result = s.rotate(90, origin=Point(0, 0))
        self.check_sgpd_equals_gpd(result, expected)

        # Test origin='centroid'
        result = s.rotate(45, origin="centroid")
        expected = gpd.GeoSeries(
            [
                Point(1.0, 1.0),
                LineString(
                    [
                        (1.3535533905932737, -0.8535533905932737),
                        (0.6464466094067263, -0.14644660940672627),
                    ]
                ),
                Polygon(
                    [
                        (3.8047378541243653, -0.9428090415820636),
                        (3.8047378541243653, 0.4714045207910314),
                        (2.3905242917512703, 0.4714045207910314),
                        (3.8047378541243653, -0.9428090415820636),
                    ]
                ),
                None,
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Test invalid origin strings
        with pytest.raises((ValueError, TypeError)):
            s.rotate(90, origin="invalid")

    @pytest.mark.parametrize(
        "scale_kwargs",
        [
            pytest.param({}, id="defaults"),
            pytest.param(
                {
                    "xfact": np.float64(2),
                    "yfact": np.int64(3),
                    "zfact": np.float32(4),
                    "origin": "center",
                },
                id="custom-center",
            ),
            pytest.param(
                {"xfact": 2, "yfact": 3, "zfact": 4, "origin": "centroid"},
                id="custom-centroid",
            ),
            pytest.param(
                {"xfact": -1, "yfact": 2, "zfact": -3, "origin": Point(1, 1)},
                id="negative-point-origin",
            ),
            pytest.param(
                {"xfact": 0, "yfact": 0, "zfact": 0, "origin": (0, 0)},
                id="zero-tuple-origin",
            ),
        ],
    )
    def test_scale_factors_and_origins(self, scale_kwargs):
        geoms = [
            Point(2, 3),
            LineString([(1, 2), (4, 6)]),
            # This asymmetric polygon distinguishes bbox center from centroid.
            Polygon([(0, 0), (4, 0), (0, 2), (0, 0)]),
        ]
        source = GeoSeries(geoms)
        expected = gpd.GeoSeries(geoms).scale(**scale_kwargs)

        result = source.scale(**scale_kwargs)

        self.check_sgpd_equals_gpd(result, expected)

    def test_scale_3d_default_origin(self):
        result = GeoSeries([Point(1, 2, 3)]).scale(xfact=2, yfact=3, zfact=4)

        point = result.to_geopandas().iloc[0]
        # Keyword origins are 2D, so the point is its own x/y origin while
        # z is scaled around zero.
        assert tuple(point.coords[0]) == pytest.approx((1.0, 2.0, 12.0))

    @pytest.mark.parametrize(
        "origin,expected_coordinates",
        [
            pytest.param((1, 1), [(1, 4, 12), (7, 16, 36)], id="two-coordinate-tuple"),
            pytest.param(Point(1, 1), [(1, 4, 12), (7, 16, 36)], id="2d-point"),
            pytest.param(
                (1, 1, 2), [(1, 4, 6), (7, 16, 30)], id="three-coordinate-tuple"
            ),
            pytest.param(Point(1, 1, 2), [(1, 4, 6), (7, 16, 30)], id="3d-point"),
        ],
    )
    def test_scale_3d_explicit_origin(self, origin, expected_coordinates):
        source = GeoSeries([LineString([(1, 2, 3), (4, 6, 9)])])

        result = source.scale(xfact=2, yfact=3, zfact=4, origin=origin)

        line = result.to_geopandas().iloc[0]
        assert list(line.coords) == pytest.approx(expected_coordinates)

    def test_scale_2d_stays_2d_with_3d_parameters(self):
        geoms = [
            Point(1, 2),
            LineString([(0, 0), (2, 1)]),
            Polygon([(0, 0), (2, 0), (1, 1), (0, 0)]),
        ]
        source = GeoSeries(geoms)
        expected = gpd.GeoSeries(geoms).scale(2, 3, 4, origin=(1, 1, 2))

        result = source.scale(2, 3, 4, origin=(1, 1, 2))

        self.check_sgpd_equals_gpd(result, expected)
        assert not any(geom.has_z for geom in result.to_geopandas())

    def test_scale_preserves_metadata_empty_null_and_delegates(self):
        geoms = [
            Point(1, 2),
            LineString([(0, 0), (2, 1)]),
            Point(),
            LineString(),
            Polygon(),
            None,
        ]
        index = pd.Index(
            ["point", "line", "empty-point", "empty-line", "empty-polygon", "null"],
            name="feature_id",
        )
        source = GeoSeries(geoms, index=index, crs="EPSG:3857", name="geometry")
        expected = gpd.GeoSeries(
            geoms, index=index, crs="EPSG:3857", name="geometry"
        ).scale(2, 3, 4, origin="center")

        result = source.scale(2, 3, 4, origin="center")

        self.check_sgpd_equals_gpd(result, expected)
        assert result.name is None
        assert result.crs == source.crs == expected.crs
        actual = result.to_geopandas()
        assert actual.loc["empty-point"].is_empty
        assert actual.loc["empty-point"].geom_type == "Point"
        assert actual.loc["empty-line"].is_empty
        assert actual.loc["empty-line"].geom_type == "LineString"
        assert actual.loc["empty-polygon"].is_empty
        assert actual.loc["empty-polygon"].geom_type == "Polygon"
        assert actual.loc["null"] is None

        srids = result._internal.spark_frame.select(
            stf.ST_SRID(result.spark.column).alias("srid")
        ).collect()
        assert {row.srid for row in srids if row.srid is not None} == {3857}

        frame_result = source.to_geoframe().scale(2, 3, 4, origin="center")
        assert isinstance(frame_result, GeoSeries)
        self.check_sgpd_equals_gpd(frame_result, expected)
        assert frame_result.crs == source.crs

    def test_scale_validates_factors_and_origin(self):
        source = GeoSeries([Point(1, 2)])

        invalid_factors = [
            ("xfact", None),
            ("yfact", "2"),
            ("zfact", np.array([2])),
            ("xfact", [2]),
            ("yfact", 2 + 1j),
        ]
        for name, value in invalid_factors:
            with pytest.raises(TypeError, match=rf"'{name}' must be a numeric scalar"):
                source.scale(**{name: value})

        with pytest.raises(TypeError, match="'xfact' must be a numeric scalar"):
            source.scale(xfact=ps.Series([2.0]))

        for origin in ("invalid", "CENTER"):
            with pytest.raises(ValueError, match="origin must be"):
                source.scale(origin=origin)

        for origin in ((1,), (1, 2, 3, 4)):
            with pytest.raises(
                ValueError, match="tuple must contain two or three coordinates"
            ):
                source.scale(origin=origin)

        for origin in ((1, "2"), (1, None), (1, 2, "invalid-z")):
            with pytest.raises(TypeError, match="only numeric coordinates"):
                source.scale(origin=origin)

        for origin in (None, [0, 0], Polygon([(0, 0), (1, 0), (0, 1)])):
            with pytest.raises(TypeError, match="origin must be"):
                source.scale(origin=origin)

        with pytest.raises(ValueError, match="Point must be a non-empty 2D or 3D"):
            source.scale(origin=Point())

        # Operation-wide arguments are validated even when there is no
        # non-empty geometry on which to apply the transformation.
        empty_source = GeoSeries([Polygon(), None])
        with pytest.raises(TypeError, match="'xfact' must be a numeric scalar"):
            empty_source.scale(xfact=None)
        with pytest.raises(ValueError, match="origin must be"):
            empty_source.scale(origin="invalid")

    def test_skew_documented_examples(self):
        geoms = [
            Point(1, 1),
            LineString([(1, -1), (1, 0)]),
            Polygon([(3, -1), (4, 0), (3, 1), (3, -1)]),
        ]
        source = GeoSeries(geoms)

        for kwargs in (
            {"xs": 45, "ys": 30},
            {"xs": 45, "ys": 30, "origin": (0, 0)},
        ):
            result = source.skew(**kwargs)
            expected = gpd.GeoSeries(geoms).skew(**kwargs)
            self.check_sgpd_equals_gpd(result, expected)

    @pytest.mark.parametrize(
        "xs,ys",
        [
            pytest.param(45, 0, id="x-only"),
            pytest.param(0, 30, id="y-only"),
            pytest.param(45, 30, id="both"),
            pytest.param(-45, -30, id="negative"),
            pytest.param(0, 0, id="zero"),
        ],
    )
    def test_skew_angle_combinations(self, xs, ys):
        geoms = [
            Point(1, 2),
            LineString([(0, 0), (2, 4)]),
            Polygon([(0, 0), (4, 0), (0, 2), (0, 0)]),
        ]
        source = GeoSeries(geoms)
        expected = gpd.GeoSeries(geoms).skew(xs, ys, origin=(0, 0))

        result = source.skew(xs, ys, origin=(0, 0))

        self.check_sgpd_equals_gpd(result, expected)

    def test_skew_degrees_radians_and_180_degree_clamp(self):
        import math

        source = GeoSeries([LineString([(0, 0), (2, 4)])])

        degrees = source.skew(45, 30, origin=(0, 0)).to_geopandas().iloc[0]
        radians = (
            source.skew(
                math.pi / 4,
                math.pi / 6,
                origin=(0, 0),
                use_radians=True,
            )
            .to_geopandas()
            .iloc[0]
        )
        numpy_bool_radians = (
            source.skew(
                math.pi / 4,
                math.pi / 6,
                origin=(0, 0),
                use_radians=np.bool_(True),
            )
            .to_geopandas()
            .iloc[0]
        )
        clamped = source.skew(180, 180, origin=(0, 0)).to_geopandas().iloc[0]

        expected = [(0.0, 0.0), (6.0, 5.1547005383792515)]
        assert list(degrees.coords) == pytest.approx(expected)
        assert list(radians.coords) == pytest.approx(expected)
        assert list(numpy_bool_radians.coords) == pytest.approx(expected)
        assert list(clamped.coords) == [(0.0, 0.0), (2.0, 4.0)]

    @pytest.mark.parametrize(
        "use_radians",
        [
            pytest.param(0, id="int-false"),
            pytest.param(1, id="int-true"),
            pytest.param(np.int64(0), id="numpy-int-false"),
            pytest.param(np.int64(1), id="numpy-int-true"),
        ],
    )
    def test_skew_accepts_integer_use_radians(self, use_radians):
        source = GeoSeries([LineString([(0, 0), (2, 4)])])
        kwargs = {
            "xs": 0.5,
            "ys": -0.25,
            "origin": (0, 0),
            "use_radians": use_radians,
        }

        result = source.skew(**kwargs)
        expected = source.to_geopandas().skew(**kwargs)

        self.check_sgpd_equals_gpd(result, expected)

    def test_skew_center_and_centroid_origins(self):
        source = GeoSeries([Polygon([(0, 0), (4, 0), (0, 2), (0, 0)])])

        centered = source.skew(45, 30, origin="center").to_geopandas().iloc[0]
        centroid = source.skew(45, 30, origin="centroid").to_geopandas().iloc[0]

        assert centered.exterior.coords[0] == pytest.approx((-1.0, -1.1547005383792512))
        assert centroid.exterior.coords[0] == pytest.approx(
            (-2.0 / 3.0, -0.7698003589195008)
        )

    @pytest.mark.parametrize(
        "origin",
        [
            pytest.param((1, 1), id="two-coordinate-tuple"),
            pytest.param((1, 1, 999), id="three-coordinate-tuple"),
            pytest.param(Point(1, 1), id="2d-point"),
            pytest.param(Point(1, 1, 999), id="3d-point"),
        ],
    )
    def test_skew_explicit_origins_ignore_z(self, origin):
        source = GeoSeries([LineString([(1, 2, 3), (4, 6, 9)])])

        result = source.skew(45, 45, origin=origin)

        line = result.to_geopandas().iloc[0]
        assert line.has_z
        assert list(line.coords) == pytest.approx([(2, 2, 3), (9, 9, 9)])
        assert [coordinate[2] for coordinate in line.coords] == [3.0, 9.0]

    def test_skew_2d_stays_2d_with_3d_origin(self):
        geoms = [
            Point(1, 2),
            LineString([(0, 0), (2, 1)]),
            Polygon([(0, 0), (2, 0), (1, 1), (0, 0)]),
        ]
        source = GeoSeries(geoms)
        expected = gpd.GeoSeries(geoms).skew(45, 30, origin=(1, 1, 999))

        result = source.skew(45, 30, origin=(1, 1, 999))

        self.check_sgpd_equals_gpd(result, expected)
        assert not any(geom.has_z for geom in result.to_geopandas())

    @pytest.mark.parametrize("origin", ["center", "centroid"])
    def test_skew_preserves_metadata_empty_null_and_delegates(self, origin):
        geoms = [
            Point(1, 2),
            LineString([(0, 0), (2, 1)]),
            Point(),
            LineString(),
            Polygon(),
            None,
        ]
        index = pd.Index(
            ["point", "line", "empty-point", "empty-line", "empty-polygon", "null"],
            name="feature_id",
        )
        source = GeoSeries(geoms, index=index, crs="EPSG:3857", name="geometry")
        expected = gpd.GeoSeries(
            geoms, index=index, crs="EPSG:3857", name="geometry"
        ).skew(45, 30, origin=origin)

        result = source.skew(45, 30, origin=origin)

        self.check_sgpd_equals_gpd(result, expected)
        assert result.name is None
        assert result.crs == source.crs == expected.crs
        actual = result.to_geopandas()
        assert actual.loc["empty-point"].is_empty
        assert actual.loc["empty-point"].geom_type == "Point"
        assert actual.loc["empty-line"].is_empty
        assert actual.loc["empty-line"].geom_type == "LineString"
        assert actual.loc["empty-polygon"].is_empty
        assert actual.loc["empty-polygon"].geom_type == "Polygon"
        assert actual.loc["null"] is None

        srids = result._internal.spark_frame.select(
            stf.ST_SRID(result.spark.column).alias("srid")
        ).collect()
        assert {row.srid for row in srids if row.srid is not None} == {3857}

        frame_result = source.to_geoframe().skew(45, 30, origin=origin)
        assert isinstance(frame_result, GeoSeries)
        self.check_sgpd_equals_gpd(frame_result, expected)
        assert frame_result.crs == source.crs

    def test_skew_validates_angles_units_and_origin(self):
        source = GeoSeries([Point(1, 2)])

        invalid_angles = [
            ("xs", None),
            ("ys", "30"),
            ("xs", np.array([45])),
            ("ys", [30]),
            ("xs", 45 + 1j),
        ]
        for name, value in invalid_angles:
            with pytest.raises(TypeError, match=rf"'{name}' must be a numeric scalar"):
                source.skew(**{name: value})

        for name in ("xs", "ys"):
            with pytest.raises(TypeError, match=rf"'{name}' must be a numeric scalar"):
                source.skew(**{name: ps.Series([45.0])})

        for use_radians in (None, 0.0, 1.0, "true", [True], np.array(True)):
            with pytest.raises(TypeError, match="'use_radians' must be a boolean"):
                source.skew(use_radians=use_radians)

        for origin in ("invalid", "CENTER"):
            with pytest.raises(ValueError, match="origin must be"):
                source.skew(origin=origin)

        for origin in ((1,), (1, 2, 3, 4)):
            with pytest.raises(
                ValueError, match="tuple must contain two or three coordinates"
            ):
                source.skew(origin=origin)

        for origin in ((1, "2"), (1, None), (1, 2, "ignored-by-skew")):
            with pytest.raises(TypeError, match="only numeric coordinates"):
                source.skew(origin=origin)

        for origin in (
            None,
            [0, 0],
            Polygon([(0, 0), (1, 0), (0, 1)]),
            ps.Series([0.0, 0.0]),
        ):
            with pytest.raises(TypeError, match="origin must be"):
                source.skew(origin=origin)

        with pytest.raises(ValueError, match="Point must be a non-empty 2D or 3D"):
            source.skew(origin=Point())

        # Operation-wide arguments are validated even when there is no
        # non-empty geometry on which to apply the transformation.
        empty_source = GeoSeries([Polygon(), None])
        for name in ("xs", "ys"):
            with pytest.raises(TypeError, match=rf"'{name}' must be a numeric scalar"):
                empty_source.skew(**{name: None})
        with pytest.raises(TypeError, match="'use_radians' must be a boolean"):
            empty_source.skew(use_radians=1.0)
        with pytest.raises(ValueError, match="origin must be"):
            empty_source.skew(origin="invalid")

    def test_translate_documented_example_and_delegation(self):
        geoms = [
            Point(1, 1),
            LineString([(1, -1), (1, 0)]),
            Polygon([(3, -1), (4, 0), (3, 1)]),
            None,
        ]
        s = GeoSeries(geoms)

        # Docstring example: translate(2, 3)
        result = s.translate(2, 3)
        expected = gpd.GeoSeries(
            [
                Point(3, 4),
                LineString([(3, 2), (3, 3)]),
                Polygon([(5, 2), (6, 3), (5, 4), (5, 2)]),
                None,
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        df_result = s.to_geoframe().translate(2, 3)
        assert isinstance(df_result, GeoSeries)
        self.check_sgpd_equals_gpd(df_result, expected)

    @pytest.mark.parametrize(
        "offsets",
        [
            pytest.param((), id="defaults"),
            pytest.param((2, -3, 0), id="integer-xy"),
            pytest.param((-1.5, 0.25, -2.0), id="negative-fractional"),
            pytest.param(
                (np.float64(2), np.int64(3), np.float32(5)),
                id="numpy-scalars",
            ),
        ],
    )
    def test_translate_offsets_and_dimensions(self, offsets):
        geoms = [
            Point(1, 2),
            LineString([(0, 0), (2, 4)]),
            Point(1, 2, 3),
            LineString([(0, 0, 1), (2, 4, 7)]),
        ]
        source = GeoSeries(geoms)
        expected = gpd.GeoSeries(geoms).translate(*offsets)

        result = source.translate(*offsets)

        self.check_sgpd_equals_gpd(result, expected)
        actual = result.to_geopandas().sort_index()
        assert not actual.iloc[0].has_z
        assert not actual.iloc[1].has_z
        assert actual.iloc[2].has_z
        assert actual.iloc[3].has_z

    def test_translate_preserves_metadata_empty_and_null(self):
        geoms = [
            Point(1, 2),
            Point(),
            LineString(),
            Polygon(),
            None,
        ]
        index = pd.Index(
            ["point", "empty-point", "empty-line", "empty-polygon", "null"],
            name="feature_id",
        )
        source = GeoSeries(geoms, index=index, crs="EPSG:3857", name="geometry")
        expected = gpd.GeoSeries(
            geoms, index=index, crs="EPSG:3857", name="geometry"
        ).translate(2, -3, 5)

        result = source.translate(2, -3, 5)

        self.check_sgpd_equals_gpd(result, expected)
        assert result.name is None
        assert result.crs == source.crs == expected.crs
        actual = result.to_geopandas()
        assert actual.loc["empty-point"].is_empty
        assert actual.loc["empty-point"].geom_type == "Point"
        assert actual.loc["empty-line"].is_empty
        assert actual.loc["empty-line"].geom_type == "LineString"
        assert actual.loc["empty-polygon"].is_empty
        assert actual.loc["empty-polygon"].geom_type == "Polygon"
        assert actual.loc["null"] is None

        srids = result._internal.spark_frame.select(
            stf.ST_SRID(result.spark.column).alias("srid")
        ).collect()
        assert {row.srid for row in srids if row.srid is not None} == {3857}

        frame_result = source.to_geoframe().translate(2, -3, 5)
        assert isinstance(frame_result, GeoSeries)
        self.check_sgpd_equals_gpd(frame_result, expected)
        assert frame_result.crs == source.crs

    def test_translate_validates_offsets(self):
        source = GeoSeries([Point(1, 2)])
        invalid_offsets = [
            ("xoff", None),
            ("yoff", "2"),
            ("zoff", np.array([2])),
            ("xoff", np.array(2.0)),
            ("yoff", [2]),
            ("zoff", 2 + 1j),
            ("xoff", pd.Series([2.0])),
        ]
        for name, value in invalid_offsets:
            with pytest.raises(TypeError, match=rf"'{name}' must be a numeric scalar"):
                source.translate(**{name: value})

        for value in (ps.Series([2.0]), source.spark.column):
            for name in ("xoff", "yoff", "zoff"):
                with pytest.raises(
                    TypeError, match=rf"'{name}' must be a numeric scalar"
                ):
                    source.translate(**{name: value})

        # Operation-wide arguments are validated even when there is no
        # non-empty geometry on which to apply the transformation.
        empty_source = GeoSeries([Point(), LineString(), Polygon(), None])
        for name in ("xoff", "yoff", "zoff"):
            with pytest.raises(TypeError, match=rf"'{name}' must be a numeric scalar"):
                empty_source.translate(**{name: None})

    def test_force_2d(self):
        s = sgpd.GeoSeries(
            [
                Point(0, -1, 2.5),  # 3D point
                LineString([(0, 0, 1), (1, 1, 2)]),  # 3D line
                Polygon([(0, 0, 1), (1, 0, 2), (1, 1, 3), (0, 0, 1)]),  # 3D polygon
                Point(5, 5),  # already 2D
                Polygon(),  # empty geometry
                None,  # None preserved
                shapely.wkt.loads("POINT M (1 2 3)"),
                shapely.wkt.loads("LINESTRING ZM (1 2 3 4, 5 6 7 8)"),
            ]
        )

        result = s.force_2d()

        expected = gpd.GeoSeries(
            [
                Point(0, -1),
                LineString([(0, 0), (1, 1)]),
                Polygon([(0, 0), (1, 0), (1, 1), (0, 0)]),
                Point(5, 5),
                Polygon(),
                None,
                shapely.wkt.loads("POINT (1 2)"),
                shapely.wkt.loads("LINESTRING (1 2, 5 6)"),
            ]
        )

        self.check_sgpd_equals_gpd(result, expected)

        df_result = s.to_geoframe().force_2d()
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_force_3d(self):
        # 1. 2D geometries promoted to 3D with default z=0.0
        s = sgpd.GeoSeries(
            [
                Point(1, 2),
                Point(0.5, 2.5, 2),
                Point(1, 1, np.nan),
                LineString([(1, 1), (0, 1), (1, 0)]),
                Polygon([(0, 0), (0, 10), (10, 10)]),
                GeometryCollection(
                    [
                        Point(1, 1),
                        LineString([(1, 1), (0, 1), (1, 0)]),
                    ]
                ),
            ]
        )
        # Promote 2D to 3D with z=0, keep 3D as is
        expected = gpd.GeoSeries(
            [
                Point(1, 2, 0),
                Point(0.5, 2.5, 2),
                Point(1, 1, 0),
                LineString([(1, 1, 0), (0, 1, 0), (1, 0, 0)]),
                Polygon([(0, 0, 0), (0, 10, 0), (10, 10, 0), (0, 0, 0)]),
                GeometryCollection(
                    [
                        Point(1, 1, 0),
                        LineString([(1, 1, 0), (0, 1, 0), (1, 0, 0)]),
                    ]
                ),
            ]
        )
        result = s.force_3d()
        self.check_sgpd_equals_gpd(result, expected)

        # 2. 2D geometries promoted to 3D with scalar z
        expected = gpd.GeoSeries(
            [
                Point(1, 2, 4),
                Point(0.5, 2.5, 2),
                Point(1, 1, 4),
                LineString([(1, 1, 4), (0, 1, 4), (1, 0, 4)]),
                Polygon([(0, 0, 4), (0, 10, 4), (10, 10, 4), (0, 0, 4)]),
                GeometryCollection(
                    [
                        Point(1, 1, 4),
                        LineString([(1, 1, 4), (0, 1, 4), (1, 0, 4)]),
                    ]
                ),
            ]
        )
        result = s.force_3d(4)
        self.check_sgpd_equals_gpd(result, expected)

        # 3. Array-like z: use ps.Series
        z = [0, 2, 2, 3, 4, 5]
        expected = gpd.GeoSeries(
            [
                Point(1, 2, 0),
                Point(0.5, 2.5, 2),
                Point(1, 1, 2),
                LineString([(1, 1, 3), (0, 1, 3), (1, 0, 3)]),
                Polygon([(0, 0, 4), (0, 10, 4), (10, 10, 4), (0, 0, 4)]),
                GeometryCollection(
                    [
                        Point(1, 1, 5),
                        LineString([(1, 1, 5), (0, 1, 5), (1, 0, 5)]),
                    ]
                ),
            ]
        )
        result = s.force_3d(z)
        self.check_sgpd_equals_gpd(result, expected)

        # 4. Ensure M and ZM geometries are handled correctly
        s = sgpd.GeoSeries(
            [
                shapely.wkt.loads("POINT M (1 2 3)"),
                shapely.wkt.loads("POINT ZM (1 2 3 4)"),
            ]
        )
        result = s.force_3d(7.5)
        expected = gpd.GeoSeries(
            [
                shapely.wkt.loads("POINT Z (1 2 7.5)"),
                shapely.wkt.loads("POINT Z (1 2 3)"),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

    def test_line_merge(self):
        s = GeoSeries(
            [
                MultiLineString([[(0, 0), (1, 1)], [(1, 1), (2, 2)]]),
                MultiLineString([[(0, 0), (1, 1)], [(2, 2), (3, 3)]]),
                LineString([(0, 0), (1, 1)]),
                None,
            ]
        )
        expected = gpd.GeoSeries(
            [
                MultiLineString([[(0, 0), (1, 1)], [(1, 1), (2, 2)]]),
                MultiLineString([[(0, 0), (1, 1)], [(2, 2), (3, 3)]]),
                LineString([(0, 0), (1, 1)]),
                None,
            ]
        ).line_merge()
        result = s.line_merge()
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().line_merge()
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_build_area(self):
        # build_area is an aggregate operation: all linework is combined,
        # then areas are built from the combined noded linework.
        s = GeoSeries(
            [
                LineString([(0, 0), (1, 0)]),
                LineString([(1, 0), (0.5, 1)]),
                LineString([(0.5, 1), (0, 0)]),
            ]
        )
        result = s.build_area()
        assert result.name == "polygons"
        assert len(result) == 1
        expected_poly = Polygon([(1, 0), (0, 0), (0.5, 1), (1, 0)])
        self.check_geom_equals(result.iloc[0], expected_poly)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().build_area()
        assert df_result.name == "polygons"
        assert len(df_result) == 1
        self.check_geom_equals(df_result.iloc[0], expected_poly)

        # Test empty GeoSeries
        result_empty = GeoSeries([]).build_area()
        assert len(result_empty) == 0
        assert result_empty.name == "polygons"

    def test_polygonize(self):
        # polygonize is an aggregate operation: all linework is combined,
        # then polygons are formed from the combined noded linework.
        s = GeoSeries(
            [
                LineString([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]),
                LineString([(1, 0), (2, 0), (2, 1), (1, 1)]),
            ]
        )
        result = s.polygonize()
        assert result.name == "polygons"
        assert len(result) == 2

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().polygonize()
        assert df_result.name == "polygons"
        assert len(df_result) == 2

        # Test that full=True raises NotImplementedError
        with pytest.raises(NotImplementedError):
            s.polygonize(full=True)

        # Test empty GeoSeries
        result_empty = GeoSeries([]).polygonize()
        assert len(result_empty) == 0
        assert result_empty.name == "polygons"

    def test_unary_union(self):
        s = GeoSeries([box(0, 0, 1, 1), box(0, 0, 2, 2)])
        with pytest.warns(FutureWarning, match="unary_union"):
            result = s.unary_union
        expected = Polygon([(0, 1), (0, 2), (2, 2), (2, 0), (1, 0), (0, 0), (0, 1)])
        self.check_geom_equals(result, expected)

        # Check that GeoDataFrame works too
        with pytest.warns(FutureWarning, match="unary_union"):
            df_result = s.to_geoframe().unary_union
        self.check_geom_equals(df_result, expected)

    def test_union_all(self):
        s = GeoSeries([box(0, 0, 1, 1), box(0, 0, 2, 2)])
        result = s.union_all()
        expected = Polygon([(0, 1), (0, 2), (2, 2), (2, 0), (1, 0), (0, 0), (0, 1)])
        self.check_geom_equals(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().union_all()
        self.check_geom_equals(df_result, expected)

        # Empty GeoSeries
        s = sgpd.GeoSeries([])
        result = s.union_all()
        expected = GeometryCollection()
        self.check_geom_equals(result, expected)

    def test_row_wise_dataframe(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                Polygon([(0, 0), (1, 1), (0, 1)]),
                Polygon([(0, 0), (1, 1), (0, 1)]),
            ]
        )
        s2 = GeoSeries([Point(-5.5, 1), Point(1, 2), Point(3, 1)])

        # self: GeoSeries, other: GeoDataFrame
        expected = pd.Series([5.5, 1, 2])
        result = s.distance(s2.to_geoframe())
        self.check_pd_series_equal(result, expected)

        # self: GeoDataFrame, other: GeoDataFrame
        result = s.to_geoframe().distance(s2.to_geoframe())
        self.check_pd_series_equal(result, expected)

        # Same but for overlay
        expected = gpd.GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1), (0, 0)]),
                Polygon([(0, 0), (1, 1), (0, 1), (0, 0)]),
                Polygon([(0, 0), (1, 1), (0, 1), (0, 0)]),
            ]
        )
        result = s.difference(s2.to_geoframe())
        self.check_sgpd_equals_gpd(result, expected)

        result = s.to_geoframe().difference(s2.to_geoframe())
        self.check_sgpd_equals_gpd(result, expected)

    def test_crosses(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (2, 2), (0, 2)]),
                LineString([(0, 0), (2, 2)]),
                LineString([(2, 0), (0, 2)]),
                Point(0, 1),
            ],
        )
        s2 = GeoSeries(
            [
                LineString([(1, 0), (1, 3)]),
                LineString([(2, 0), (0, 2)]),
                Point(1, 1),
                Point(0, 1),
            ],
            index=range(1, 5),
        )

        line = LineString([(-1, 1), (3, 1)])
        result = s.crosses(line)
        expected = pd.Series([True, True, True, False])
        self.check_pd_series_equal(result, expected)

        result = s.crosses(s2, align=True)
        expected = pd.Series([False, True, False, False, False])
        self.check_pd_series_equal(result, expected)

        result = s.crosses(s2, align=False)
        expected = pd.Series([True, True, False, False])
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().crosses(s2, align=False)
        self.check_pd_series_equal(df_result, expected)

        # The underlying ST_Crosses expression returns NULL for GeometryCollection
        # (https://github.com/apache/sedona/issues/2417), which the GeoSeries
        # predicate normalizes to False to match GeoPandas' boolean contract.
        # Ensure M-dimension doesn't break things.
        s = GeoSeries(
            [
                wkt.loads("GEOMETRYCOLLECTION M (POINT M (1 2 3))"),
                wkt.loads("LINESTRING M (0 0 1, 1 1 2)"),
            ]
        )
        line = LineString([(0, 0), (1, 1)])
        result = s.crosses(line)
        expected = pd.Series([False, False], dtype=bool)
        self.check_pd_series_equal(result, expected)

    def test_disjoint(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (2, 2), (0, 2)]),
                LineString([(0, 0), (2, 2)]),
                Point(0, 0),
                Point(5, 5),
            ],
        )
        s2 = GeoSeries(
            [
                Point(3, 3),
                Point(1, 1),
                Point(0, 0),
                Point(0, 0),
            ],
        )
        result = s.disjoint(s2, align=False)
        expected = pd.Series([True, False, False, True])
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().disjoint(s2, align=False)
        self.check_pd_series_equal(df_result, expected)

    def test_intersects(self):
        s = sgpd.GeoSeries(
            [
                Polygon([(0, 0), (2, 2), (0, 2)]),
                LineString([(0, 0), (2, 2)]),
                LineString([(2, 0), (0, 2)]),
                Point(0, 1),
            ],
        )
        s2 = sgpd.GeoSeries(
            [
                LineString([(1, 0), (1, 3)]),
                LineString([(2, 0), (0, 2)]),
                Point(1, 1),
                Point(-100, -100),
            ],
        )

        result = s.intersects(s2)
        expected = pd.Series([True, True, True, False])
        self.check_pd_series_equal(result, expected)

        line = LineString([(-1, 1), (3, 1)])
        result = s.intersects(line)
        expected = pd.Series([True, True, True, True])
        self.check_pd_series_equal(result, expected)

        # from the original doc string
        s2 = sgpd.GeoSeries(
            [
                LineString([(1, 0), (1, 3)]),
                LineString([(2, 0), (0, 2)]),
                Point(1, 1),
                Point(0, 1),
            ],
            index=range(1, 5),
        )

        result = s.intersects(s2, align=True)
        expected = pd.Series([False, True, True, False, False])

        result = s.intersects(s2, align=False)
        expected = pd.Series([True, True, True, True])

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().intersects(s2, align=False)
        self.check_pd_series_equal(df_result, expected)

    def test_overlaps(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (2, 2), (0, 2)]),
                Polygon([(0, 0), (2, 2), (0, 2)]),
                LineString([(0, 0), (2, 2)]),
                MultiPoint([(0, 0), (0, 1)]),
            ],
        )
        s2 = GeoSeries(
            [
                Polygon([(0, 0), (2, 0), (0, 2)]),
                LineString([(0, 1), (1, 1)]),
                LineString([(1, 1), (3, 3)]),
                Point(0, 1),
            ],
            index=range(1, 5),
        )

        polygon = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        result = s.overlaps(polygon)
        expected = pd.Series([True, True, False, False])
        self.check_pd_series_equal(result, expected)

        result = s.overlaps(s2, align=True)
        expected = pd.Series([False, True, False, False, False])
        self.check_pd_series_equal(result, expected)

        result = s.overlaps(s2, align=False)
        expected = pd.Series([True, False, True, False])
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().overlaps(s2, align=False)
        self.check_pd_series_equal(df_result, expected)

    def test_touches(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (2, 2), (0, 2)]),
                Polygon([(0, 0), (2, 2), (0, 2)]),
                LineString([(0, 0), (2, 2)]),
                MultiPoint([(0, 0), (0, 1)]),
            ],
        )
        s2 = GeoSeries(
            [
                Polygon([(0, 0), (-2, 0), (0, -2)]),
                LineString([(0, 1), (1, 1)]),
                LineString([(1, 1), (3, 0)]),
                Point(0, 1),
            ],
            index=range(1, 5),
        )
        line = LineString([(0, 0), (-1, -2)])
        result = s.touches(line)
        expected = pd.Series([True, True, True, True])
        self.check_pd_series_equal(result, expected)

        result = s.touches(s2, align=True)
        expected = pd.Series([False, True, True, False, False])
        self.check_pd_series_equal(result, expected)

        result = s.touches(s2, align=False)
        expected = pd.Series([True, False, True, False])
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().touches(s2, align=False)
        self.check_pd_series_equal(df_result, expected)

    def test_within(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (2, 2), (0, 2)]),
                Polygon([(0, 0), (1, 2), (0, 2)]),
                LineString([(0, 0), (0, 2)]),
                Point(0, 1),
            ],
        )
        s2 = GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                LineString([(0, 0), (0, 2)]),
                LineString([(0, 0), (0, 1)]),
                Point(0, 1),
            ],
            index=range(1, 5),
        )

        polygon = Polygon([(0, 0), (2, 2), (0, 2)])
        result = s.within(polygon)
        expected = pd.Series([True, True, False, False])
        self.check_pd_series_equal(result, expected)

        result = s2.within(s, align=True)
        expected = pd.Series([False, False, True, False, False])
        self.check_pd_series_equal(result, expected)

        result = s2.within(s, align=False)
        expected = pd.Series([True, False, True, True], index=range(1, 5))
        self.check_pd_series_equal(result, expected)

        # Ensure we return False if either geometries are empty
        s = GeoSeries([Point(), Point(), Polygon(), Point(0, 1)])
        result = s.within(s2, align=False)
        expected = pd.Series([False, False, False, True])
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().within(s2, align=False)
        self.check_pd_series_equal(df_result, expected)

    def test_covers(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
                Polygon([(0, 0), (2, 2), (0, 2)]),
                LineString([(0, 0), (2, 2)]),
                Point(0, 0),
            ],
        )
        s2 = GeoSeries(
            [
                Polygon([(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5)]),
                Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
                LineString([(1, 1), (1.5, 1.5)]),
                Point(0, 0),
            ],
            index=range(1, 5),
        )

        poly = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
        result = s.covers(poly)
        expected = pd.Series([True, False, False, False])
        self.check_pd_series_equal(result, expected)

        result = s.covers(s2, align=True)
        expected = pd.Series([False, False, False, False, False])
        self.check_pd_series_equal(result, expected)

        result = s.covers(s2, align=False)
        expected = pd.Series([True, False, True, True])
        self.check_pd_series_equal(result, expected)

        # Ensure we return False if either geometries are empty
        s = GeoSeries([Point(), Point(), Polygon(), Point(0, 0)])
        result = s.covers(s2, align=False)
        expected = pd.Series([False, False, False, True])
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().covers(s2, align=False)
        self.check_pd_series_equal(df_result, expected)

    def test_covered_by(self):
        s = GeoSeries(
            [
                Polygon([(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5)]),
                Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
                LineString([(1, 1), (1.5, 1.5)]),
                Point(0, 0),
            ],
        )
        s2 = GeoSeries(
            [
                Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
                Polygon([(0, 0), (2, 2), (0, 2)]),
                LineString([(0, 0), (2, 2)]),
                Point(0, 0),
            ],
            index=range(1, 5),
        )

        poly = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
        result = s.covered_by(poly)
        expected = pd.Series([True, True, True, True])
        self.check_pd_series_equal(result, expected)

        result = s.covered_by(s2, align=True)
        expected = pd.Series([False, True, True, True, False])
        self.check_pd_series_equal(result, expected)

        result = s.covered_by(s2, align=False)
        expected = pd.Series([True, False, True, True])
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().covered_by(s2, align=False)
        self.check_pd_series_equal(df_result, expected)

    def test_distance(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (1, 0), (1, 1)]),
                Polygon([(0, 0), (-1, 0), (-1, 1)]),
                LineString([(1, 1), (0, 0)]),
                Point(0, 0),
            ],
        )
        s2 = GeoSeries(
            [
                Polygon([(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5)]),
                Point(3, 1),
                LineString([(1, 0), (2, 0)]),
                Point(0, 1),
            ],
            index=range(1, 5),
        )
        point = Point(-1, 0)
        result = s.distance(point)
        expected = pd.Series([1.0, 0.0, 1.0, 1.0])
        self.check_pd_series_equal(result, expected)

        result = s.distance(s2, align=True)
        expected = pd.Series([np.nan, 0.707107, 2.000000, 1.000000, np.nan])
        self.check_pd_series_equal(result, expected)

        result = s.distance(s2, align=False)
        expected = pd.Series([0.000000, 3.162278, 0.707107, 1.000000])
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().distance(s2, align=False)
        self.check_pd_series_equal(df_result, expected)

    def test_intersection(self):
        s = sgpd.GeoSeries(
            [
                Polygon([(0, 0), (2, 2), (0, 2)]),
                Polygon([(0, 0), (2, 2), (0, 2)]),
                LineString([(0, 0), (2, 2)]),
                LineString([(2, 0), (0, 2)]),
                Point(0, 1),
            ],
        )

        geom = Polygon(
            [(-0.5, -0.5), (-0.5, 2.5), (2.5, 2.5), (2.5, -0.5), (-0.5, -0.5)]
        )
        result = s.intersection(geom)
        result.sort_index(inplace=True)
        expected = gpd.GeoSeries(
            [
                Polygon([(0, 0), (2, 2), (0, 2)]),
                Polygon([(0, 0), (2, 2), (0, 2)]),
                LineString([(0, 0), (2, 2)]),
                LineString([(2, 0), (0, 2)]),
                Point(0, 1),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        s2 = sgpd.GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                LineString([(1, 0), (1, 3)]),
                LineString([(2, 0), (0, 2)]),
                Point(1, 1),
                Point(-100, -100),
            ],
        )
        result = s.intersection(s2)
        result.sort_index(inplace=True)
        expected = gpd.GeoSeries(
            [
                Polygon([(0, 0), (0, 1), (1, 1), (0, 0)]),
                LineString([(1, 1), (1, 2)]),
                Point(1, 1),
                Point(1, 1),
                Point(),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # from the original doc string
        s = sgpd.GeoSeries(
            [
                Polygon([(0, 0), (2, 2), (0, 2)]),
                Polygon([(0, 0), (2, 2), (0, 2)]),
                LineString([(0, 0), (2, 2)]),
                LineString([(2, 0), (0, 2)]),
                Point(0, 1),
            ],
        )
        s2 = sgpd.GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                LineString([(1, 0), (1, 3)]),
                LineString([(2, 0), (0, 2)]),
                Point(1, 1),
                Point(0, 1),
            ],
            index=range(1, 6),
            crs=4326,
        )

        # Ensure the index is preserved when crs is set (previously an issue)
        expected_index = ps.Index(range(1, 6))
        with ps.option_context("compute.ops_on_diff_frames", True):
            assert s2.index.equals(expected_index)

        result = s.intersection(s2, align=True)
        result.sort_index(inplace=True)
        expected = gpd.GeoSeries(
            [
                None,
                Polygon([(0, 0), (0, 1), (1, 1), (0, 0)]),
                Point(1, 1),
                LineString([(2, 0), (0, 2)]),
                Point(),
                None,
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().intersection(s2, align=True)
        df_result.sort_index(inplace=True)
        self.check_sgpd_equals_gpd(df_result, expected)

        result = s2.intersection(s, align=False)
        result.sort_index(inplace=True)
        expected = gpd.GeoSeries(
            [
                Polygon([(0, 0), (0, 1), (1, 1), (0, 0)]),
                LineString([(1, 1), (1, 2)]),
                Point(1, 1),
                Point(1, 1),
                Point(0, 1),
            ],
            index=range(1, 6),  # left's index
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Ensure result of align=False retains the left's index
        assert result.index.to_pandas().equals(expected.index)

        # Check that GeoDataFrame works too
        df_result = s2.to_geoframe().intersection(s, align=False)
        df_result.sort_index(inplace=True)
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_snap(self):
        s = GeoSeries(
            [
                Point(0.5, 2.5),
                LineString([(0.1, 0.1), (0.49, 0.51), (1.01, 0.89)]),
                Polygon([(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]),
            ],
        )
        s2 = GeoSeries(
            [
                Point(0, 2),
                LineString([(0, 0), (0.5, 0.5), (1.0, 1.0)]),
                Point(8, 10),
            ],
            index=range(1, 4),
        )
        result = s.snap(Point(0, 2), tolerance=1)
        expected = gpd.GeoSeries(
            [
                Point(0, 2),
                LineString([(0.1, 0.1), (0.49, 0.51), (1.01, 0.89)]),
                Polygon([(0, 0), (0, 2), (0, 10), (10, 10), (10, 0), (0, 0)]),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Note: This test result slightly differs from the original geopandas's result,
        # which doesn't include the Point(0, 0) in the Polygon below.
        result = s.snap(s2, tolerance=1, align=True)
        expected = gpd.GeoSeries(
            [
                None,
                LineString([(0.1, 0.1), (0.49, 0.51), (1.01, 0.89)]),
                Polygon(
                    [(0, 0), (0.5, 0.5), (1, 1), (0, 10), (10, 10), (10, 0), (0, 0)]
                ),
                None,
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        result = s.snap(s2, tolerance=1, align=False)
        expected = gpd.GeoSeries(
            [
                Point(0, 2),
                LineString([(0, 0), (0.5, 0.5), (1, 1)]),
                Polygon([(0, 0), (0, 10), (8, 10), (10, 10), (10, 0), (0, 0)]),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().snap(s2, tolerance=1, align=False)
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_shortest_line(self):
        s1 = GeoSeries(
            [
                Point(0, 0),
                LineString([(0, 0), (1, 0)]),
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            ]
        )
        s2 = GeoSeries(
            [
                Point(1, 1),
                Point(0, 1),
                Point(2, 2),
            ]
        )

        result = s1.shortest_line(s2, align=False)
        expected = gpd.GeoSeries(
            [
                LineString([(0, 0), (1, 1)]),
                LineString([(0, 0), (0, 1)]),
                LineString([(1, 1), (2, 2)]),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Test with single geometry
        result = s1.shortest_line(Point(1, 1))
        expected = gpd.GeoSeries(
            [
                LineString([(0, 0), (1, 1)]),
                LineString([(1, 0), (1, 1)]),
                LineString([(1, 1), (1, 1)]),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Test that GeoDataFrame works too
        df_result = s1.to_geoframe().shortest_line(s2, align=False)
        expected = gpd.GeoSeries(
            [
                LineString([(0, 0), (1, 1)]),
                LineString([(0, 0), (0, 1)]),
                LineString([(1, 1), (2, 2)]),
            ]
        )
        self.check_sgpd_equals_gpd(df_result, expected)

    @requires_geopandas_shared_paths
    def test_shared_paths(self):
        reference = LineString([(0, 0), (2, 0), (2, 1)])
        geometries = [
            LineString([(0, 0), (2, 0), (2, 2)]),
            LineString([(2, 2), (2, 0), (0, 0)]),
            MultiLineString(
                [
                    [(0, 0), (2, 0)],
                    [(2, 2), (2, 1)],
                ]
            ),
            LineString(),
            None,
        ]
        index = pd.MultiIndex.from_tuples(
            [
                ("same", 1),
                ("opposite", 2),
                ("multi", 3),
                ("empty", 4),
                ("null", 5),
            ],
            names=["kind", "row"],
        )
        source = GeoSeries(
            geometries,
            index=index,
            crs="EPSG:3857",
            name="roads",
        )
        expected = gpd.GeoSeries(
            geometries,
            index=index,
            crs="EPSG:3857",
            name="roads",
        ).shared_paths(reference)

        result = source.shared_paths(reference)

        self.check_sgpd_equals_gpd(result, expected)
        assert result.name is None
        assert result.crs == source.crs == expected.crs
        actual = result.to_geopandas()
        for label in index[:-1]:
            actual_collection = actual.loc[label]
            expected_collection = expected.loc[label]
            assert actual_collection.geom_type == "GeometryCollection"
            assert len(actual_collection.geoms) == 2
            for component in range(2):
                assert actual_collection.geoms[component].equals(
                    expected_collection.geoms[component]
                )
        assert actual.loc[("null", 5)] is None

        srids = result._internal.spark_frame.select(
            stf.ST_SRID(result.spark.column).alias("srid")
        ).collect()
        assert {row.srid for row in srids if row.srid is not None} == {3857}

        frame_source = GeoSeries(
            geometries,
            index=pd.Index(["same", "opposite", "multi", "empty", "null"]),
            crs="EPSG:3857",
        )
        frame_expected = gpd.GeoSeries(
            geometries,
            index=pd.Index(["same", "opposite", "multi", "empty", "null"]),
            crs="EPSG:3857",
        ).shared_paths(reference)
        frame_result = frame_source.to_geoframe().shared_paths(reference)
        assert isinstance(frame_result, GeoSeries)
        self.check_sgpd_equals_gpd(frame_result, frame_expected)
        assert frame_result.crs == frame_source.crs

        with pytest.raises(TypeError, match="'other' must be"):
            source.shared_paths(None)

    def test_shared_paths_avoids_unnecessary_srid_copies(self, monkeypatch):
        left = GeoSeries(
            [LineString([(0, 0), (2, 0)])],
            crs="EPSG:3857",
        )
        right = GeoSeries(
            [LineString([(0, 0), (1, 0)])],
            crs="EPSG:3857",
        )
        reference = LineString([(0, 0), (1, 0)])
        original_set_srid = stf.ST_SetSRID
        set_srid_arguments = []

        def recording_set_srid(geometry, srid):
            set_srid_arguments.append(srid)
            return original_set_srid(geometry, srid)

        monkeypatch.setattr(stf, "ST_SetSRID", recording_set_srid)

        matching_result = left.shared_paths(right, align=False)
        assert set_srid_arguments == []
        assert matching_result.to_geopandas().iloc[0] is not None

        scalar_result = left.shared_paths(reference)
        assert set_srid_arguments == [3857]
        assert scalar_result.to_geopandas().iloc[0] is not None

    @requires_geopandas_shared_paths
    def test_shared_paths_duplicate_multiindex_alignment(self):
        left_index = pd.MultiIndex.from_tuples(
            [("a", 1), ("a", 1), ("b", 2)], names=["group", "row"]
        )
        right_index = pd.MultiIndex.from_tuples(
            [("a", 1), ("a", 1), ("c", 3)], names=["group", "row"]
        )
        left_geometries = [
            LineString([(0, 0), (2, 0)]),
            LineString([(2, 0), (0, 0)]),
            LineString([(0, 1), (2, 1)]),
        ]
        right_geometries = [
            LineString([(0, 0), (1, 0)]),
            LineString([(2, 0), (1, 0)]),
            LineString([(0, 3), (2, 3)]),
        ]
        left = GeoSeries(left_geometries, index=left_index, crs="EPSG:4326")
        right = GeoSeries(right_geometries, index=right_index, crs="EPSG:4326")
        expected_left = gpd.GeoSeries(
            left_geometries, index=left_index, crs="EPSG:4326"
        )
        expected_right = gpd.GeoSeries(
            right_geometries, index=right_index, crs="EPSG:4326"
        )

        result = left.shared_paths(right, align=True)
        expected = expected_left.shared_paths(expected_right, align=True)

        actual = result.to_geopandas()
        assert len(actual) == 6
        pd.testing.assert_index_equal(actual.index, expected.index)
        assert result.crs == left.crs
        for actual_collection, expected_collection in zip(actual, expected):
            if actual_collection is None or expected_collection is None:
                assert actual_collection is None and expected_collection is None
                continue
            for component in range(2):
                assert actual_collection.geoms[component].equals(
                    expected_collection.geoms[component]
                )

        positional_result = left.shared_paths(right, align=False)
        positional_expected = expected_left.shared_paths(expected_right, align=False)
        positional_actual = positional_result.to_geopandas()
        pd.testing.assert_index_equal(
            positional_actual.index, positional_expected.index
        )
        for actual_collection, expected_collection in zip(
            positional_actual, positional_expected
        ):
            for component in range(2):
                assert actual_collection.geoms[component].equals(
                    expected_collection.geoms[component]
                )

    @requires_geopandas_shared_paths
    def test_shared_paths_default_alignment_warning_and_length_validation(self):
        left = GeoSeries(
            [LineString([(0, 0), (2, 0)])],
            index=pd.Index(["left"], name="feature"),
        )
        right = GeoSeries(
            [LineString([(0, 0), (1, 0)])],
            index=pd.Index(["right"], name="feature"),
        )

        with pytest.warns(
            UserWarning,
            match="The indices of the left and right GeoSeries' are not equal",
        ):
            result = left.shared_paths(right)
        expected = gpd.GeoSeries(
            [LineString([(0, 0), (2, 0)])],
            index=pd.Index(["left"], name="feature"),
        ).shared_paths(
            gpd.GeoSeries(
                [LineString([(0, 0), (1, 0)])],
                index=pd.Index(["right"], name="feature"),
            ),
            align=True,
        )
        self.check_sgpd_equals_gpd(result, expected)

        with pytest.warns(UserWarning, match="CRS mismatch") as warning_info:
            crs_result = GeoSeries(
                [LineString([(0, 0), (2, 0)])], crs="EPSG:4326"
            ).shared_paths(
                GeoSeries([LineString([(0, 0), (1, 0)])], crs="EPSG:3857"),
                align=False,
            )
        crs_warning = next(
            warning
            for warning in warning_info
            if "CRS mismatch" in str(warning.message)
        )
        assert crs_warning.filename == __file__
        assert crs_result.crs.to_epsg() == 4326
        with pytest.warns(UserWarning, match="CRS mismatch"):
            crs_expected = gpd.GeoSeries(
                [LineString([(0, 0), (2, 0)])], crs="EPSG:4326"
            ).shared_paths(
                gpd.GeoSeries([LineString([(0, 0), (1, 0)])], crs="EPSG:3857"),
                align=False,
            )
        self.check_sgpd_equals_gpd(crs_result, crs_expected)

        with pytest.raises(
            ValueError,
            match=r"Lengths of inputs do not match\. Left: 1, Right: 2",
        ):
            left.shared_paths(
                GeoSeries(
                    [
                        LineString([(0, 0), (1, 0)]),
                        LineString([(0, 1), (1, 1)]),
                    ]
                ),
                align=False,
            )

    def test_intersection_all(self):
        s = GeoSeries([box(0, 0, 2, 2), box(1, 1, 3, 3)])
        result = s.intersection_all()
        expected = Polygon([(1, 1), (1, 2), (2, 2), (2, 1), (1, 1)])
        self.check_geom_equals(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().intersection_all()
        self.check_geom_equals(df_result, expected)

        # Empty GeoSeries
        s = sgpd.GeoSeries([])
        result = s.intersection_all()
        expected = GeometryCollection()
        self.check_geom_equals(result, expected)

    def test_contains(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (1, 1), (0, 1)]),
                LineString([(0, 0), (0, 2)]),
                LineString([(0, 0), (0, 1)]),
                Point(0, 1),
            ],
            index=range(0, 4),
        )
        s2 = GeoSeries(
            [
                Polygon([(0, 0), (2, 2), (0, 2)]),
                Polygon([(0, 0), (1, 2), (0, 2)]),
                LineString([(0, 0), (0, 2)]),
                Point(0, 1),
            ],
            index=range(1, 5),
        )

        point = Point(0, 1)
        result = s.contains(point)
        expected = pd.Series([False, True, False, True])
        self.check_pd_series_equal(result, expected)

        result = s2.contains(s, align=True)
        expected = pd.Series([False, False, False, True, False])
        self.check_pd_series_equal(result, expected)

        result = s2.contains(s, align=False)
        expected = pd.Series([True, False, True, True], index=range(1, 5))
        self.check_pd_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = s2.to_geoframe().contains(s, align=False)
        self.check_pd_series_equal(df_result, expected)

    def test_contains_properly(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
                Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
                Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
            ]
        )
        s2 = GeoSeries(
            [
                Point(1, 1),  # interior point → True
                Point(0, 0),  # boundary point → False
                Point(3, 3),  # exterior point → False
            ]
        )

        result = s.contains_properly(s2, align=False)
        expected = pd.Series([True, False, False])
        self.check_pd_series_equal(result, expected)

        # Test with single geometry
        result = s.contains_properly(Point(1, 1))
        expected = pd.Series([True, True, True])
        self.check_pd_series_equal(result, expected)

        # Test that GeoDataFrame works too
        df_result = s.to_geoframe().contains_properly(s2, align=False)
        expected = pd.Series([True, False, False])
        self.check_pd_series_equal(df_result, expected)

    def test_relate(self):
        s = GeoSeries(
            [
                Point(0, 0),
                Point(0, 0),
                LineString([(0, 0), (1, 1)]),
            ]
        )
        s2 = GeoSeries(
            [
                Point(0, 0),
                Point(1, 1),
                LineString([(0, 0), (1, 1)]),
            ]
        )
        # "ABCDEFGHI" DE-9 Format
        # A Dimension of intersection
        # B Dimension of interior intersection
        # C Dimension of boundary intersection
        # D Interior of first geometry intersects exterior of second
        # E Exterior of first geometry intersects interior of second
        # F Boundary of first geometry intersects exterior of second
        # G Exterior of first geometry intersects boundary of second
        # H Exterior of first geometry intersects exterior of second
        # I Dimension of intersection for interiors
        # 0 = false, 1 = point, 2 = line, F = area

        # 1. Test with single geometry
        point = Point(0, 0)
        result = s.relate(point)
        expected = pd.Series(["0FFFFFFF2", "0FFFFFFF2", "FF10F0FF2"])
        self.check_pd_series_equal(result, expected)

        result = s.relate(s2)
        expected = pd.Series(["0FFFFFFF2", "FF0FFF0F2", "1FFF0FFF2"])
        self.check_pd_series_equal(result, expected)
        # 2. Test with align=True (different indices)
        s3 = GeoSeries(
            [
                Point(0, 0),
                Point(1, 1),
            ],
            index=range(1, 3),
        )
        s4 = GeoSeries(
            [
                Point(0, 0),
                Point(1, 1),
            ],
            index=range(0, 2),
        )
        result = s3.relate(s4, align=True)
        expected = pd.Series([None, "FF0FFF0F2", None], index=[0, 1, 2])
        self.check_pd_series_equal(result, expected)

        # 3. Test with align=False
        result = s3.relate(s4, align=False)
        expected = pd.Series(["0FFFFFFF2", "0FFFFFFF2"], index=range(1, 3))
        self.check_pd_series_equal(result, expected)

        # 4. Check that GeoDataFrame works too
        df_result = s.to_geoframe().relate(s2, align=False)
        expected = pd.Series(["0FFFFFFF2", "FF0FFF0F2", "1FFF0FFF2"])
        self.check_pd_series_equal(df_result, expected)

        # 5. touching_polygons and overlapping polygon case
        touching_poly_a = Polygon(((0, 0), (1, 0), (1, 1), (0, 1), (0, 0)))
        touching_poly_b = Polygon(((1, 0), (2, 0), (2, 1), (1, 1), (1, 0)))
        overlapping_poly_a = Polygon(((0, 0), (2, 0), (2, 2), (0, 2), (0, 0)))
        overlapping_poly_b = Polygon(((1, 1), (3, 1), (3, 3), (1, 3), (1, 1)))
        s5 = GeoSeries([touching_poly_a, overlapping_poly_a])
        s6 = GeoSeries([touching_poly_b, overlapping_poly_b])
        result = s5.relate(s6)

        expected = pd.Series(["FF2F11212", "212101212"])
        self.check_pd_series_equal(result, expected)

    def test_relate_pattern(self):
        s = GeoSeries(
            [
                Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
                Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
                Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
            ]
        )
        s2 = GeoSeries(
            [
                Point(1, 1),  # interior → contains pattern matches
                Point(0, 0),  # boundary → contains pattern fails
                Point(3, 3),  # exterior → contains pattern fails
            ]
        )

        # Test contains_properly pattern: T**FF*FF*
        result = s.relate_pattern(s2, "T**FF*FF*", align=False)
        expected = pd.Series([True, False, False])
        self.check_pd_series_equal(result, expected)

        # Test intersects pattern: T********
        result = s.relate_pattern(s2, "T********", align=False)
        expected = pd.Series([True, False, False])
        self.check_pd_series_equal(result, expected)

        # Test with single geometry
        result = s.relate_pattern(Point(1, 1), "T**FF*FF*")
        expected = pd.Series([True, True, True])
        self.check_pd_series_equal(result, expected)

        # Test that GeoDataFrame works too
        df_result = s.to_geoframe().relate_pattern(s2, "T**FF*FF*", align=False)
        expected = pd.Series([True, False, False])
        self.check_pd_series_equal(df_result, expected)

    def test_frechet_distance(self):
        s1 = GeoSeries(
            [
                LineString([(0, 0), (1, 0), (2, 0)]),
                LineString([(0, 0), (1, 1)]),
            ]
        )
        s2 = GeoSeries(
            [
                LineString([(0, 1), (1, 2), (2, 1)]),
                LineString([(1, 0), (2, 1)]),
            ]
        )

        result = s1.frechet_distance(s2, align=False)
        expected = pd.Series([2.0, 1.0])
        self.check_pd_series_equal(result, expected)

        # Test with single geometry
        line = LineString([(0, 1), (1, 2), (2, 1)])
        result = s1.frechet_distance(line)
        expected = pd.Series([2.0, 1.0])
        self.check_pd_series_equal(result, expected)

        # Test that GeoDataFrame works too
        df_result = s1.to_geoframe().frechet_distance(s2, align=False)
        expected = pd.Series([2.0, 1.0])
        self.check_pd_series_equal(df_result, expected)

        # Test that densify raises NotImplementedError
        with pytest.raises(NotImplementedError):
            s1.frechet_distance(s2, densify=0.5)

    def test_hausdorff_distance(self):
        s1 = GeoSeries(
            [
                LineString([(0, 0), (1, 0), (2, 0)]),
                LineString([(0, 0), (1, 1)]),
            ]
        )
        s2 = GeoSeries(
            [
                LineString([(0, 1), (1, 2), (2, 1)]),
                LineString([(1, 0), (2, 1)]),
            ]
        )

        result = s1.hausdorff_distance(s2, align=False)
        expected = pd.Series([2.0, 1.0])
        self.check_pd_series_equal(result, expected)

        # Test with single geometry
        line = LineString([(0, 1), (1, 2), (2, 1)])
        result = s1.hausdorff_distance(line)
        expected = pd.Series([2.0, 1.0])
        self.check_pd_series_equal(result, expected)

        # Test that GeoDataFrame works too
        df_result = s1.to_geoframe().hausdorff_distance(s2, align=False)
        expected = pd.Series([2.0, 1.0])
        self.check_pd_series_equal(df_result, expected)

        # Test with densify parameter
        result = s1.hausdorff_distance(s2, densify=0.5, align=False)
        expected = pd.Series([2.0, 1.0])
        self.check_pd_series_equal(result, expected)

    def test_geom_equals(self):
        s1 = GeoSeries(
            [
                Point(0, 0),
                Point(1, 1),
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            ]
        )
        s2 = GeoSeries(
            [
                Point(0, 0),
                Point(2, 2),
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            ]
        )

        result = s1.geom_equals(s2, align=False)
        expected = pd.Series([True, False, True])
        self.check_pd_series_equal(result, expected)

        # Test with single geometry
        result = s1.geom_equals(Point(0, 0))
        expected = pd.Series([True, False, False])
        self.check_pd_series_equal(result, expected)

        # Test that GeoDataFrame works too
        df_result = s1.to_geoframe().geom_equals(s2, align=False)
        expected = pd.Series([True, False, True])
        self.check_pd_series_equal(df_result, expected)

    @pytest.mark.parametrize(
        ("method", "kwargs"),
        [
            ("geom_equals", {}),
            ("geom_equals_exact", {"tolerance": 0}),
        ],
    )
    def test_equality_predicates_warn_on_crs_mismatch(self, method, kwargs):
        left = GeoSeries([Point(0, 0)], crs="EPSG:4326")

        right = GeoSeries([Point(0, 0)], crs="EPSG:3857")
        right_operands = [
            right,
            right.to_geoframe(),
            right.to_spark_pandas(),
            GeoSeries([Point(0, 0)]),
        ]
        for right_operand in right_operands:
            with pytest.warns(UserWarning, match="CRS mismatch") as warning_info:
                result = getattr(left, method)(
                    right_operand,
                    align=False,
                    **kwargs,
                )

            crs_warnings = [
                warning
                for warning in warning_info
                if "CRS mismatch" in str(warning.message)
            ]
            assert len(crs_warnings) == 1
            assert crs_warnings[0].filename == __file__
            self.check_pd_series_equal(result, pd.Series([True]))

        with pytest.warns(UserWarning, match="CRS mismatch") as warning_info:
            frame_result = getattr(left.to_geoframe(), method)(
                right,
                align=False,
                **kwargs,
            )
        crs_warnings = [
            warning
            for warning in warning_info
            if "CRS mismatch" in str(warning.message)
        ]
        assert len(crs_warnings) == 1
        assert crs_warnings[0].filename == __file__
        self.check_pd_series_equal(frame_result, pd.Series([True]))

    @pytest.mark.parametrize(
        ("method", "kwargs"),
        [
            ("geom_equals", {}),
            ("geom_equals_exact", {"tolerance": 0}),
        ],
    )
    def test_equality_predicates_avoid_spurious_crs_warnings(self, method, kwargs):
        left = GeoSeries([Point(0, 0)], crs="EPSG:4326")
        right = GeoSeries([Point(0, 0)], crs="EPSG:4326")

        with warnings.catch_warnings(record=True) as warning_info:
            warnings.simplefilter("always")
            matching_result = getattr(left, method)(right, align=False, **kwargs)
            spark_pandas_result = getattr(left, method)(
                right.to_spark_pandas(),
                align=False,
                **kwargs,
            )
            scalar_result = getattr(left, method)(Point(0, 0), **kwargs)
            crsless_result = getattr(GeoSeries([Point(0, 0)]), method)(
                GeoSeries([Point(0, 0)]),
                align=False,
                **kwargs,
            )

        assert not any(
            "CRS mismatch" in str(warning.message) for warning in warning_info
        )
        self.check_pd_series_equal(matching_result, pd.Series([True]))
        self.check_pd_series_equal(spark_pandas_result, pd.Series([True]))
        self.check_pd_series_equal(scalar_result, pd.Series([True]))
        self.check_pd_series_equal(crsless_result, pd.Series([True]))

    @pytest.mark.parametrize(
        ("method", "kwargs"),
        [
            ("geom_equals", {}),
            ("geom_equals_exact", {"tolerance": 0}),
        ],
    )
    def test_equality_predicate_crs_warning_preserves_alignment(self, method, kwargs):
        left = GeoSeries([Point(0, 0)], index=[1], crs="EPSG:4326")
        right = GeoSeries([Point(0, 0)], index=[0], crs="EPSG:3857")

        with pytest.warns(UserWarning, match="CRS mismatch") as warning_info:
            result = getattr(left, method)(right, align=True, **kwargs)

        crs_warnings = [
            warning
            for warning in warning_info
            if "CRS mismatch" in str(warning.message)
        ]
        assert len(crs_warnings) == 1
        assert crs_warnings[0].filename == __file__
        self.check_pd_series_equal(result, pd.Series([False, False], index=[0, 1]))

    @pytest.mark.parametrize(
        ("method", "kwargs"),
        [
            ("geom_equals", {}),
            ("geom_equals_exact", {"tolerance": 0}),
        ],
    )
    def test_equality_predicate_alignment_warning_stays_at_user_call(
        self, method, kwargs
    ):
        left_index = pd.MultiIndex.from_tuples(
            [("left", 0)],
            names=["side", "row"],
        )
        right_index = pd.MultiIndex.from_tuples(
            [("right", 0)],
            names=["side", "row"],
        )
        left = GeoSeries([Point(0, 0)], index=left_index)
        right = GeoSeries([Point(0, 0)], index=right_index)

        with pytest.warns(
            UserWarning,
            match="indices of the left and right GeoSeries",
        ) as warning_info:
            getattr(left, method)(right, **kwargs)

        alignment_warning = next(
            warning
            for warning in warning_info
            if "indices of the left and right GeoSeries" in str(warning.message)
        )
        assert alignment_warning.filename == __file__

    def test_binary_operation_with_projected_multiindex(self):
        index = pd.MultiIndex.from_tuples([("a", 1), ("b", 2)], names=["group", "row"])
        result = GeoSeries([Point(0, 0), Point(1, 1)], index=index).geom_equals(
            Point(0, 0)
        )
        expected = pd.Series([True, False], index=index)
        self.check_pd_series_equal(result, expected)

    def test_binary_operation_single_index_alignment_stays_lazy(self, monkeypatch):
        left_geometries = [box(0, 0, 2, 2), box(3, 3, 5, 5)]
        right_geometries = [box(1, 1, 4, 4), box(0, 0, 1, 1)]
        left = GeoSeries(left_geometries, index=["a", "b"])
        right = GeoSeries(right_geometries, index=["b", "a"])
        sequence_attachments = []
        original_attach = InternalFrame.attach_distributed_sequence_column

        def recording_attach(sdf, column_name):
            sequence_attachments.append(column_name)
            return original_attach(sdf, column_name)

        monkeypatch.setattr(
            InternalFrame,
            "attach_distributed_sequence_column",
            recording_attach,
        )

        result = left.intersection(right, align=True)
        assert sequence_attachments == []
        expected = gpd.GeoSeries(left_geometries, index=["a", "b"]).intersection(
            gpd.GeoSeries(right_geometries, index=["b", "a"]), align=True
        )
        self.check_sgpd_equals_gpd(result, expected)

    @pytest.mark.parametrize(
        ("method", "kwargs"),
        [
            ("intersection", {}),
            ("shortest_line", {}),
            ("snap", {"tolerance": 0.25}),
        ],
    )
    def test_binary_geometry_operations_preserve_duplicate_multiindex(
        self, method, kwargs
    ):
        left_index = pd.MultiIndex.from_tuples(
            [("a", 1), ("a", 1), ("b", 2)], names=["group", "row"]
        )
        right_index = pd.MultiIndex.from_tuples(
            [("a", 1), ("a", 1), ("c", 3)], names=["group", "row"]
        )
        left = GeoSeries(
            [
                LineString([(0, 0), (2, 0)]),
                LineString([(0, 0), (2, 0)]),
                LineString([(0, 1), (2, 1)]),
            ],
            index=left_index,
        )
        right = GeoSeries(
            [
                LineString([(0, 0), (1, 0)]),
                LineString([(1, 0), (2, 0)]),
                LineString([(0, 3), (2, 3)]),
            ],
            index=right_index,
        )

        result = getattr(left, method)(right, align=True, **kwargs)
        expected = getattr(gpd.GeoSeries(left.to_geopandas()), method)(
            gpd.GeoSeries(right.to_geopandas()),
            align=True,
            **kwargs,
        )

        self.check_sgpd_equals_gpd(result, expected)
        assert len(result) == 6

        identical_right = GeoSeries(
            right.to_geopandas().array,
            index=left_index,
        )
        identical_result = getattr(left, method)(identical_right, align=True, **kwargs)
        identical_expected = getattr(gpd.GeoSeries(left.to_geopandas()), method)(
            gpd.GeoSeries(identical_right.to_geopandas()),
            align=True,
            **kwargs,
        )

        self.check_sgpd_equals_gpd(identical_result, identical_expected)
        assert len(identical_result) == 3

    def test_geom_equals_exact(self):
        s = GeoSeries([Point(0, 1.1), Point(0, 1.0), Point(0, 1.2)])

        result = s.geom_equals_exact(Point(0, 1), tolerance=0.1)
        expected = gpd.GeoSeries(
            [Point(0, 1.1), Point(0, 1.0), Point(0, 1.2)]
        ).geom_equals_exact(Point(0, 1), tolerance=0.1)
        self.check_pd_series_equal(result, expected)

        result = s.geom_equals_exact(Point(0, 1), tolerance=0.15)
        expected = gpd.GeoSeries(
            [Point(0, 1.1), Point(0, 1.0), Point(0, 1.2)]
        ).geom_equals_exact(Point(0, 1), tolerance=0.15)
        self.check_pd_series_equal(result, expected)

        df_result = s.to_geoframe().geom_equals_exact(Point(0, 1), tolerance=0.15)
        self.check_pd_series_equal(df_result, expected)

    def test_geom_equals_exact_alignment(self):
        left_geometries = [Point(0, 0), Point(1, 1), None]
        right_geometries = [Point(1, 1), Point(0, 0), Point(9, 9)]
        left_index = ["a", "b", "c"]
        right_index = ["b", "a", "d"]

        left = GeoSeries(left_geometries, index=left_index)
        right = GeoSeries(right_geometries, index=right_index)
        expected_left = gpd.GeoSeries(left_geometries, index=left_index)
        expected_right = gpd.GeoSeries(right_geometries, index=right_index)

        result = left.geom_equals_exact(right, tolerance=0)
        expected = expected_left.geom_equals_exact(
            expected_right, tolerance=0, align=True
        )
        self.check_pd_series_equal(result, expected)

        result = left.geom_equals_exact(right, tolerance=0, align=True)
        self.check_pd_series_equal(result, expected)

        result = left.geom_equals_exact(right, tolerance=0, align=False)
        expected = expected_left.geom_equals_exact(
            expected_right, tolerance=0, align=False
        )
        self.check_pd_series_equal(result, expected)

    def test_geom_equals_exact_duplicate_index_alignment(self):
        index = ["a", "a"]
        left_geometries = [Point(0, 0), Point(1, 1)]
        right_geometries = [Point(0, 0), Point(9, 9)]

        result = GeoSeries(left_geometries, index=index).geom_equals_exact(
            GeoSeries(right_geometries, index=index), tolerance=0, align=True
        )
        expected = gpd.GeoSeries(left_geometries, index=index).geom_equals_exact(
            gpd.GeoSeries(right_geometries, index=index),
            tolerance=0,
            align=True,
        )
        self.check_pd_series_equal(result, expected)

    def test_geom_equals_exact_unequal_duplicate_index_alignment(self):
        left_index = ["a", "a", "c"]
        right_index = ["a", "a", "b"]
        left_geometries = [Point(0, 0), Point(1, 1), Point(2, 2)]
        right_geometries = [Point(0, 0), Point(9, 9), Point(3, 3)]

        result = GeoSeries(left_geometries, index=left_index).geom_equals_exact(
            GeoSeries(right_geometries, index=right_index),
            tolerance=0,
            align=True,
        )
        expected = gpd.GeoSeries(left_geometries, index=left_index).geom_equals_exact(
            gpd.GeoSeries(right_geometries, index=right_index),
            tolerance=0,
            align=True,
        )
        self.check_pd_series_equal(result, expected)
        assert len(result) == 6

    def test_geom_equals_exact_align_false_requires_equal_lengths(self):
        left = GeoSeries([Point(0, 0)])
        right = GeoSeries([Point(0, 0), Point(1, 1)])

        with pytest.raises(
            ValueError,
            match=r"Lengths of inputs do not match\. Left: 1, Right: 2",
        ):
            left.geom_equals_exact(right, tolerance=0, align=False)

    def test_geom_equals_exact_preserves_multiindex(self):
        left_index = pd.MultiIndex.from_tuples(
            [("b", 2), ("a", 1)], names=["group", "row"]
        )
        right_index = pd.MultiIndex.from_tuples(
            [("a", 1), ("c", 3)], names=["group", "row"]
        )
        left_geometries = [Point(2, 2), Point(1, 1)]
        right_geometries = [Point(1, 1), Point(3, 3)]

        left = GeoSeries(left_geometries, index=left_index)
        right = GeoSeries(right_geometries, index=right_index)
        result = left.geom_equals_exact(right, tolerance=0, align=True)
        expected = gpd.GeoSeries(left_geometries, index=left_index).geom_equals_exact(
            gpd.GeoSeries(right_geometries, index=right_index),
            tolerance=0,
            align=True,
        )
        self.check_pd_series_equal(result, expected)

        positional_result = left.geom_equals_exact(right, tolerance=0, align=False)
        positional_expected = gpd.GeoSeries(
            left_geometries, index=left_index
        ).geom_equals_exact(
            gpd.GeoSeries(right_geometries, index=right_index),
            tolerance=0,
            align=False,
        )
        self.check_pd_series_equal(positional_result, positional_expected)

        scalar_result = left.geom_equals_exact(Point(1, 1), tolerance=0)
        scalar_expected = gpd.GeoSeries(
            left_geometries, index=left_index
        ).geom_equals_exact(Point(1, 1), tolerance=0)
        self.check_pd_series_equal(scalar_result, scalar_expected)

        duplicate_index = pd.MultiIndex.from_tuples(
            [("a", 1), ("a", 1)], names=["group", "row"]
        )
        duplicate_result = GeoSeries(
            [Point(0, 0), Point(1, 1)], index=duplicate_index
        ).geom_equals_exact(
            GeoSeries([Point(0, 0), Point(9, 9)], index=duplicate_index),
            tolerance=0,
            align=True,
        )
        duplicate_expected = gpd.GeoSeries(
            [Point(0, 0), Point(1, 1)], index=duplicate_index
        ).geom_equals_exact(
            gpd.GeoSeries([Point(0, 0), Point(9, 9)], index=duplicate_index),
            tolerance=0,
            align=True,
        )
        self.check_pd_series_equal(duplicate_result, duplicate_expected)

    def test_geom_equals_exact_aligns_multiindex_by_name(self):
        left_index = pd.MultiIndex.from_tuples(
            [("left-2", "b"), ("left-1", "a")],
            names=["left_row", "group"],
        )
        right_index = pd.MultiIndex.from_tuples(
            [("a", "right-3"), ("c", "right-4")],
            names=["group", "right_row"],
        )
        left_geometries = [Point(2, 2), Point(1, 1)]
        right_geometries = [Point(1, 1), Point(3, 3)]

        result = GeoSeries(left_geometries, index=left_index).geom_equals_exact(
            GeoSeries(right_geometries, index=right_index),
            tolerance=0,
            align=True,
        )
        expected = gpd.GeoSeries(left_geometries, index=left_index).geom_equals_exact(
            gpd.GeoSeries(right_geometries, index=right_index),
            tolerance=0,
            align=True,
        )
        self.check_pd_series_equal(result, expected)

    def test_geom_equals_exact_aligns_different_index_levels(self):
        simple_index = pd.Index(["b", "a", "d"], name="group")
        multiindex = pd.MultiIndex.from_tuples(
            [("b", 2), ("a", 1), ("c", 3)], names=["group", "row"]
        )
        simple_geometries = [Point(2, 2), Point(1, 1), Point(4, 4)]
        multi_geometries = [Point(2, 2), Point(9, 9), Point(3, 3)]

        result = GeoSeries(simple_geometries, index=simple_index).geom_equals_exact(
            GeoSeries(multi_geometries, index=multiindex),
            tolerance=0,
            align=True,
        )
        expected = gpd.GeoSeries(
            simple_geometries, index=simple_index
        ).geom_equals_exact(
            gpd.GeoSeries(multi_geometries, index=multiindex),
            tolerance=0,
            align=True,
        )
        self.check_pd_series_equal(result, expected)

        reverse_result = GeoSeries(
            multi_geometries, index=multiindex
        ).geom_equals_exact(
            GeoSeries(simple_geometries, index=simple_index),
            tolerance=0,
            align=True,
        )
        reverse_expected = gpd.GeoSeries(
            multi_geometries, index=multiindex
        ).geom_equals_exact(
            gpd.GeoSeries(simple_geometries, index=simple_index),
            tolerance=0,
            align=True,
        )
        self.check_pd_series_equal(reverse_result, reverse_expected)

    def test_geom_equals_exact_rejects_unrelated_multiindex_names(self):
        left_index = pd.MultiIndex.from_tuples(
            [("a", 1)], names=["left_group", "left_row"]
        )
        right_index = pd.MultiIndex.from_tuples(
            [("b", 2)], names=["right_group", "right_row"]
        )

        with pytest.raises(
            ValueError, match="cannot join with no overlapping index names"
        ):
            GeoSeries([Point(0, 0)], index=left_index).geom_equals_exact(
                GeoSeries([Point(0, 0)], index=right_index),
                tolerance=0,
                align=True,
            )

    def test_geom_equals_exact_linearring_serialization_limitation(self):
        ring = LinearRing([(0, 0), (1, 0), (1, 1), (0, 0)])
        line = LineString(ring.coords)

        # Sedona represents standalone LinearRings as LineStrings throughout
        # the GeoPandas compatibility layer.
        result = GeoSeries([ring]).geom_equals_exact(line, tolerance=0)
        self.check_pd_series_equal(result, pd.Series([True]))

    def test_geom_equals_exact_structural_null_and_dimensions(self):
        left_geometries = [
            Point(),
            LineString(),
            Polygon(),
            None,
            Point(1, 2, 3),
            wkt.loads("POINT M (1 2 3)"),
            LineString([(0, 0), (1, 1)]),
            GeometryCollection([Point(0, 0), LineString([(0, 0), (1, 1)])]),
        ]
        right_geometries = [
            Point(),
            Polygon(),
            Polygon(),
            None,
            Point(1, 2, 99),
            wkt.loads("POINT M (1 2 99)"),
            LineString([(1, 1), (0, 0)]),
            GeometryCollection([LineString([(0, 0), (1, 1)]), Point(0, 0)]),
        ]

        result = GeoSeries(left_geometries).geom_equals_exact(
            GeoSeries(right_geometries), tolerance=0, align=False
        )
        expected = gpd.GeoSeries(left_geometries).geom_equals_exact(
            gpd.GeoSeries(right_geometries), tolerance=0, align=False
        )
        self.check_pd_series_equal(result, expected)

    @pytest.mark.parametrize("tolerance", [-1.0, np.nan, np.inf])
    def test_geom_equals_exact_special_tolerances(self, tolerance):
        geometries = [Point(0, 0), Point(1, 1), None]
        result = GeoSeries(geometries).geom_equals_exact(
            Point(0, 0), tolerance=tolerance
        )
        expected = gpd.GeoSeries(geometries).geom_equals_exact(
            Point(0, 0), tolerance=tolerance
        )
        self.check_pd_series_equal(result, expected)

    @pytest.mark.parametrize("tolerance", [None, "0.1", [0.1], np.array([0.1])])
    def test_geom_equals_exact_rejects_non_scalar_tolerance(self, tolerance):
        s = GeoSeries([Point(0, 0)])
        with pytest.raises(TypeError, match="'tolerance' must be a numeric scalar"):
            s.geom_equals_exact(Point(0, 0), tolerance=tolerance)

    @pytest.mark.parametrize("other", [None, 1, "POINT (0 0)", [Point(0, 0)]])
    def test_geom_equals_exact_rejects_non_geometry_other(self, other):
        s = GeoSeries([Point(0, 0)])
        with pytest.raises(TypeError, match="'other' must be"):
            s.geom_equals_exact(other, tolerance=0)

    @requires_shapely_m_support
    def test_geom_equals_identical_structure_dimensions_and_nan(self):
        left_wkts = [
            "POINT Z (1 2 3)",
            "POINT Z (1 2 3)",
            "POINT M (1 2 3)",
            "POINT M (1 2 3)",
            "POINT Z (1 2 3)",
            "POINT ZM (1 2 3 4)",
            "POINT ZM (1 2 3 4)",
            "LINESTRING Z (0 0 1, 1 1 NaN)",
            "LINESTRING Z (0 0 1, 1 1 NaN)",
            "LINESTRING (0 0, 1 1)",
            "POLYGON ((0 0, 2 0, 0 2, 0 0))",
            "GEOMETRYCOLLECTION (POINT (0 0), LINESTRING (0 0, 1 1))",
            "POINT EMPTY",
            "POINT EMPTY",
            None,
        ]
        right_wkts = [
            "POINT Z (1 2 3)",
            "POINT Z (1 2 4)",
            "POINT M (1 2 3)",
            "POINT M (1 2 4)",
            "POINT M (1 2 3)",
            "POINT ZM (1 2 3 4)",
            "POINT ZM (1 2 3 5)",
            "LINESTRING Z (0 0 1, 1 1 NaN)",
            "LINESTRING Z (0 0 1, 1 1 2)",
            "LINESTRING (1 1, 0 0)",
            "POLYGON ((2 0, 0 2, 0 0, 2 0))",
            "GEOMETRYCOLLECTION (LINESTRING (0 0, 1 1), POINT (0 0))",
            "POINT EMPTY",
            "LINESTRING EMPTY",
            None,
        ]
        index = pd.Index(
            [
                "z-same",
                "z-different",
                "m-same",
                "m-different",
                "z-versus-m",
                "zm-same",
                "zm-different",
                "nan-same",
                "nan-different",
                "line-order",
                "ring-order",
                "part-order",
                "empty-same",
                "empty-type",
                "missing",
            ],
            name="case",
        )
        left = GeoSeries(shapely.from_wkt(left_wkts), index=index, name="left")
        right = GeoSeries(shapely.from_wkt(right_wkts), index=index, name="right")

        result = left.geom_equals_identical(right, align=False)

        self.check_pd_series_equal(
            result,
            pd.Series(
                [
                    True,
                    False,
                    True,
                    False,
                    False,
                    True,
                    False,
                    True,
                    False,
                    False,
                    False,
                    False,
                    True,
                    False,
                    False,
                ],
                index=index,
                dtype=bool,
            ),
        )
        assert result.name is None

    def test_geom_equals_identical_scalar_multiindex_geodataframe_and_plan(self):
        index = pd.MultiIndex.from_tuples(
            [("a", 1), ("b", 2), ("c", 3)], names=["group", "row"]
        )
        source = GeoSeries(
            [Point(1, 2, 3), Point(1, 2), None],
            index=index,
            name="source",
        )

        result = source.geom_equals_identical(Point(1, 2, 3))
        self.check_pd_series_equal(
            result,
            pd.Series([True, False, False], index=index, dtype=bool),
        )
        assert result.name is None

        if hasattr(result._internal.spark_frame, "_jdf"):
            plan = (
                result._internal.spark_frame._jdf.queryExecution()
                .optimizedPlan()
                .toString()
            )
            assert "BatchEvalPython" not in plan
            assert "ArrowEvalPython" not in plan
            assert "PythonUDF" not in plan

        frame_source = GeoSeries(
            [Point(1, 2, 3), Point(1, 2), None],
            index=pd.Index(["z", "xy", "missing"], name="kind"),
        )
        frame_result = frame_source.to_geoframe().geom_equals_identical(Point(1, 2, 3))
        self.check_pd_series_equal(
            frame_result,
            pd.Series(
                [True, False, False],
                index=pd.Index(["z", "xy", "missing"], name="kind"),
                dtype=bool,
            ),
        )

    @requires_shapely_m_support
    @pytest.mark.parametrize(
        ("other_wkt", "expected"),
        [
            ("POINT M (1 2 3)", [True, False, False, False, False]),
            ("POINT ZM (1 2 3 4)", [False, False, False, True, False]),
        ],
    )
    def test_geom_equals_identical_scalar_m_and_zm(self, other_wkt, expected):
        source = GeoSeries(
            shapely.from_wkt(
                [
                    "POINT M (1 2 3)",
                    "POINT M (1 2 4)",
                    "POINT Z (1 2 3)",
                    "POINT ZM (1 2 3 4)",
                    "POINT ZM (1 2 3 5)",
                ]
            )
        )

        result = source.geom_equals_identical(shapely.from_wkt(other_wkt))

        self.check_pd_series_equal(result, pd.Series(expected, dtype=bool))

    @requires_shapely_m_support
    @pytest.mark.parametrize(
        ("dimensional_wkt", "xy_wkt"),
        [
            ("POINT M EMPTY", "POINT EMPTY"),
            ("LINESTRING M EMPTY", "LINESTRING EMPTY"),
            ("POLYGON M EMPTY", "POLYGON EMPTY"),
            ("POINT ZM EMPTY", "POINT EMPTY"),
            ("LINESTRING ZM EMPTY", "LINESTRING EMPTY"),
            ("POLYGON ZM EMPTY", "POLYGON EMPTY"),
            ("LINEARRING M EMPTY", "LINEARRING EMPTY"),
            ("LINEARRING ZM EMPTY", "LINEARRING EMPTY"),
        ],
    )
    def test_geom_equals_identical_scalar_typed_empty(self, dimensional_wkt, xy_wkt):
        source = GeoSeries(shapely.from_wkt([dimensional_wkt, xy_wkt]))

        result = source.geom_equals_identical(shapely.from_wkt(dimensional_wkt))

        self.check_pd_series_equal(result, pd.Series([True, False], dtype=bool))

    def test_geom_equals_identical_alignment_and_duplicates(self):
        left_geometries = [Point(0, 0), Point(1, 1), None]
        right_geometries = [Point(1, 1), Point(0, 0), Point(9, 9)]
        left = GeoSeries(left_geometries, index=["a", "b", "c"])
        right = GeoSeries(right_geometries, index=["b", "a", "d"])

        with pytest.warns(
            UserWarning,
            match="The indices of the left and right GeoSeries' are not equal",
        ) as warning_records:
            default_result = left.geom_equals_identical(right)
        alignment_warning = next(
            warning
            for warning in warning_records
            if "indices of the left and right GeoSeries" in str(warning.message)
        )
        assert alignment_warning.filename == __file__
        aligned_expected = pd.Series(
            [True, True, False, False],
            index=pd.Index(["a", "b", "c", "d"]),
            dtype=bool,
        )
        self.check_pd_series_equal(default_result, aligned_expected)
        self.check_pd_series_equal(
            left.geom_equals_identical(right, align=True), aligned_expected
        )
        self.check_pd_series_equal(
            left.geom_equals_identical(right, align=False),
            pd.Series(
                [False, False, False],
                index=pd.Index(["a", "b", "c"]),
                dtype=bool,
            ),
        )

        duplicate_index = ["a", "a"]
        equal_duplicate_result = GeoSeries(
            [Point(0, 0), Point(1, 1)], index=duplicate_index
        ).geom_equals_identical(
            GeoSeries([Point(0, 0), Point(9, 9)], index=duplicate_index),
            align=True,
        )
        self.check_pd_series_equal(
            equal_duplicate_result,
            pd.Series([True, False], index=duplicate_index, dtype=bool),
        )

        unequal_duplicate_result = GeoSeries(
            [Point(0, 0), Point(1, 1), Point(2, 2)], index=["a", "a", "c"]
        ).geom_equals_identical(
            GeoSeries(
                [Point(0, 0), Point(9, 9), Point(3, 3)],
                index=["a", "a", "b"],
            ),
            align=True,
        )
        self.check_pd_series_equal(
            unequal_duplicate_result,
            pd.Series(
                [True, False, False, False, False, False],
                index=pd.Index(["a", "a", "a", "a", "b", "c"]),
                dtype=bool,
            ),
        )

        with pytest.raises(
            ValueError,
            match=r"Lengths of inputs do not match\. Left: 1, Right: 2",
        ):
            GeoSeries([Point(0, 0)]).geom_equals_identical(
                GeoSeries([Point(0, 0), Point(1, 1)]), align=False
            )

    def test_geom_equals_identical_preserves_named_multiindex(self):
        left_index = pd.MultiIndex.from_tuples(
            [("b", 2), ("a", 1)], names=["group", "row"]
        )
        right_index = pd.MultiIndex.from_tuples(
            [("a", 1), ("c", 3)], names=["group", "row"]
        )
        result = GeoSeries(
            [Point(2, 2), Point(1, 1)], index=left_index
        ).geom_equals_identical(
            GeoSeries([Point(1, 1), Point(3, 3)], index=right_index),
            align=True,
        )
        self.check_pd_series_equal(
            result,
            pd.Series(
                [True, False, False],
                index=pd.MultiIndex.from_tuples(
                    [("a", 1), ("b", 2), ("c", 3)], names=["group", "row"]
                ),
                dtype=bool,
            ),
        )

        unrelated_left = pd.MultiIndex.from_tuples(
            [("a", 1)], names=["left_group", "left_row"]
        )
        unrelated_right = pd.MultiIndex.from_tuples(
            [("b", 2)], names=["right_group", "right_row"]
        )
        with pytest.raises(
            ValueError, match="cannot join with no overlapping index names"
        ):
            GeoSeries([Point(0, 0)], index=unrelated_left).geom_equals_identical(
                GeoSeries([Point(0, 0)], index=unrelated_right), align=True
            )

    @requires_geopandas_geom_equals_identical
    def test_geom_equals_identical_warns_on_crs_mismatch(self):
        left = GeoSeries([Point(0, 0)], crs="EPSG:4326")
        right = GeoSeries([Point(0, 0)], crs="EPSG:3857")

        with pytest.warns(UserWarning, match="CRS mismatch"):
            result = left.geom_equals_identical(right, align=False)
        with pytest.warns(UserWarning, match="CRS mismatch"):
            expected = gpd.GeoSeries(
                [Point(0, 0)], crs="EPSG:4326"
            ).geom_equals_identical(
                gpd.GeoSeries([Point(0, 0)], crs="EPSG:3857"), align=False
            )
        self.check_pd_series_equal(result, expected)

    def test_geom_equals_identical_serialization_boundaries(self):
        empty_2d = GeoSeries.from_wkt(
            [
                "POINT EMPTY",
                "LINESTRING EMPTY",
                "POLYGON EMPTY",
                "POINT EMPTY",
                "POINT EMPTY",
            ]
        )
        empty_dimensional = GeoSeries.from_wkt(
            [
                "POINT Z EMPTY",
                "LINESTRING Z EMPTY",
                "POLYGON Z EMPTY",
                "POINT M EMPTY",
                "POINT ZM EMPTY",
            ]
        )

        # JTS cannot distinguish empty XY and XYZ sequences. M and ZM sequence
        # metadata is explicit, so the GeometryUDT serializers retain it.
        self.check_pd_series_equal(
            empty_2d.geom_equals_identical(empty_dimensional, align=False),
            pd.Series([True, True, True, False, False], dtype=bool),
        )

        nan_first_z = GeoSeries.from_wkt(["LINESTRING Z (0 0 NaN, 1 1 2)"])
        two_dimensional = GeoSeries.from_wkt(["LINESTRING (0 0, 1 1)"])

        # A later non-NaN Z now establishes and preserves the XYZ layout.
        self.check_pd_series_equal(
            nan_first_z.geom_equals_identical(two_dimensional, align=False),
            pd.Series([False], dtype=bool),
        )

        all_nan_z = GeoSeries.from_wkt(["LINESTRING Z (0 0 NaN, 1 1 NaN)"])

        # An all-NaN Z sequence remains indistinguishable from ordinary XY in JTS.
        self.check_pd_series_equal(
            all_nan_z.geom_equals_identical(two_dimensional, align=False),
            pd.Series([True], dtype=bool),
        )

        mixed_dimension = GeoSeries([MultiPoint([Point(0, 0), Point(1, 1, 2)])])
        normalized_dimension = GeoSeries(
            [shapely.from_wkt("MULTIPOINT Z ((0 0 NaN), (1 1 2))")]
        )

        # Shapely's homogeneous multi-geometry encoding selects one layout
        # before the value reaches the JVM, promoting the XY child to XYZ/NaN.
        self.check_pd_series_equal(
            mixed_dimension.geom_equals_identical(normalized_dimension, align=False),
            pd.Series([True], dtype=bool),
        )

        ring = LinearRing([(0, 0), (1, 0), (1, 1), (0, 0)])
        line = LineString(ring.coords)
        # Standalone LinearRings use the LineString representation in this layer.
        self.check_pd_series_equal(
            GeoSeries([ring]).geom_equals_identical(line),
            pd.Series([True], dtype=bool),
        )
        self.check_pd_series_equal(
            GeoSeries([LinearRing()]).geom_equals_identical(LinearRing()),
            pd.Series([True], dtype=bool),
        )

    @pytest.mark.parametrize("other", [None, 1, "POINT (0 0)", [Point(0, 0)]])
    def test_geom_equals_identical_rejects_non_geometry_other(self, other):
        with pytest.raises(TypeError, match="'other' must be"):
            GeoSeries([Point(0, 0)]).geom_equals_identical(other)

    def test_interpolate(self):
        s = GeoSeries(
            [
                LineString([(0, 0), (2, 0), (0, 2)]),
                LineString([(0, 0), (2, 2)]),
                LineString([(2, 0), (0, 2)]),
            ]
        )

        # Test with absolute distance
        result = s.interpolate(1)
        expected = gpd.GeoSeries(
            [
                Point(1, 0),
                Point(0.7071067811865476, 0.7071067811865476),
                Point(1.2928932188134524, 0.7071067811865476),
            ]
        )
        self.check_sgpd_equals_gpd(result, expected)

        # Test with normalized distance
        result = s.interpolate(0.5, normalized=True)
        expected = gpd.GeoSeries(s.to_geopandas().interpolate(0.5, normalized=True))
        self.check_sgpd_equals_gpd(result, expected)

        # Test that GeoDataFrame works too
        df_result = s.to_geoframe().interpolate(1)
        expected = gpd.GeoSeries(
            [
                Point(1, 0),
                Point(0.7071067811865476, 0.7071067811865476),
                Point(1.2928932188134524, 0.7071067811865476),
            ]
        )
        self.check_sgpd_equals_gpd(df_result, expected)

    def test_project(self):
        s = GeoSeries(
            [
                LineString([(0, 0), (2, 0), (0, 2)]),
                LineString([(0, 0), (2, 2)]),
                LineString([(2, 0), (0, 2)]),
            ]
        )

        # Test with a single point
        result = s.project(Point(1, 0))
        expected = pd.Series([1.0, 0.7071067811865476, 0.7071067811865476])
        self.check_pd_series_equal(result, expected)

        # Test with normalized=True
        result = s.project(Point(1, 0), normalized=True)
        expected = pd.Series(s.to_geopandas().project(Point(1, 0), normalized=True))
        self.check_pd_series_equal(result, expected)

        # Test with two GeoSeries
        s2 = GeoSeries(
            [
                Point(1, 0),
                Point(1, 0),
                Point(2, 1),
            ]
        )
        result = s.project(s2, align=False)
        expected = pd.Series(
            s.to_geopandas().project(gpd.GeoSeries(s2.to_geopandas()), align=False)
        )
        self.check_pd_series_equal(result, expected)

        # Test that GeoDataFrame works too
        df_result = s.to_geoframe().project(Point(1, 0))
        expected = pd.Series([1.0, 0.7071067811865476, 0.7071067811865476])
        self.check_pd_series_equal(df_result, expected)

    def test_set_crs(self):
        from pyproj import CRS

        geo_series = sgpd.GeoSeries([Point(0, 0), Point(1, 1)], name="geometry")
        assert geo_series.crs == None
        geo_series = geo_series.set_crs(epsg=4326)
        assert geo_series.crs.to_epsg() == 4326

        with pytest.raises(ValueError):
            geo_series.set_crs(4328)
        with pytest.raises(ValueError):
            geo_series.set_crs(None)

        geo_series = geo_series.set_crs(None, allow_override=True)
        assert geo_series.crs == None

        # Check that the name is preserved for set_crs
        geo_series.name = "geometry"

        inplace_result = geo_series.set_crs(4326, inplace=True)
        assert inplace_result is geo_series
        assert geo_series.crs.to_epsg() == 4326

        geo_series.crs = 3857
        assert geo_series.crs.to_epsg() == 3857

        # Check that the name is preserved for set_crs after inplace=True
        geo_series.name = "geometry"

        geo_series = sgpd.GeoSeries(self.geoseries, crs=4326)
        assert geo_series.crs.to_epsg() == 4326

        all_null = sgpd.GeoSeries([None], name="geometry", crs=4326)
        assert all_null.crs.to_epsg() == 4326
        assert all_null.copy(deep=True).crs.to_epsg() == 4326

        without_crs = all_null.set_crs(None, allow_override=True)
        assert without_crs.crs is None
        assert all_null.crs.to_epsg() == 4326

        with_other_crs = all_null.set_crs(3857, allow_override=True)
        assert with_other_crs.crs.to_epsg() == 3857
        assert all_null.crs.to_epsg() == 4326

        empty_result = sgpd.GeoSeries(
            [GeometryCollection()],
            crs=4326,
        ).explode(ignore_index=True)
        assert len(empty_result) == 0
        assert empty_result.crs.to_epsg() == 4326
        assert empty_result.set_crs(3857, allow_override=True).crs.to_epsg() == 3857

        all_null.set_crs(None, inplace=True, allow_override=True)
        assert all_null.crs is None

        custom_crs = CRS.from_proj4(
            "+proj=aeqd +lat_0=12.345 +lon_0=67.89 " "+datum=WGS84 +units=m +no_defs"
        )
        assert custom_crs.to_epsg() is None
        custom_series = sgpd.GeoSeries([Point(0, 0)]).set_crs(custom_crs)
        assert custom_series.crs == custom_crs
        assert custom_series.to_geopandas().crs == custom_crs
        assert (
            custom_series._internal.spark_frame.select(
                stf.ST_SRID(custom_series.spark.column).alias("srid")
            ).first()["srid"]
            == 0
        )

        # This test errors due to a bug in pyspark.
        # We can uncomment it once the fix is https://github.com/apache/spark/pull/51475 is merged
        # It was tested locally by using the fixed version of pyspark
        # # First element null
        # geo_series = sgpd.GeoSeries([None, None, Point(1, 1)], crs=4326)
        # assert geo_series.crs.to_epsg() == 4326

    def test_crs_metadata_propagation(self):
        source = sgpd.GeoSeries([None], name="geometry", crs=4326)

        copied = source.copy(deep=True)
        reconstructed = sgpd.GeoSeries(source)
        buffered = source.buffer(1)
        transformed = source.to_crs(3857)
        frame = source.to_geoframe()
        round_tripped = GeoDataFrame(frame.to_spark_pandas())

        source.set_crs(3857, inplace=True, allow_override=True)

        assert copied.crs.to_epsg() == 4326
        assert reconstructed.crs.to_epsg() == 4326
        assert buffered.crs.to_epsg() == 4326
        assert frame.crs.to_epsg() == 4326
        assert round_tripped.crs.to_epsg() == 4326
        assert transformed.crs.to_epsg() == 3857
