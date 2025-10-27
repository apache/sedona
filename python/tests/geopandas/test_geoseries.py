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

import shapely
import numpy as np
import pytest
import pandas as pd
import geopandas as gpd
import pyspark.pandas as ps
import sedona.spark.geopandas as sgpd
from sedona.spark.geopandas import GeoSeries, GeoDataFrame
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
from pandas.testing import assert_series_equal
import pytest
from packaging.version import parse as parse_version


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
        assert_series_equal(result.to_pandas(), ps_df.to_pandas())

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
        result = self.geoseries.area.to_pandas()
        expected = pd.Series([0.0, 0.0, 5.23, 5.23])
        assert_series_equal(result, expected)

        # Test that GeoDataFrame.area also works
        df_result = self.geoseries.to_geoframe().area.to_pandas()
        assert_series_equal(result, df_result)

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
        assert_series_equal(
            sgpd_geoseries.geometry.to_pandas(), sgpd_geoseries.to_pandas()
        )

    def test_x(self):
        geoseries = sgpd.GeoSeries(
            [Point(0, -1, 2.5), Point(2.5, 0, -1), Point(-1, 2.5, 0), Point(-1, 0)]
        )
        result = geoseries.x.to_pandas()
        expected = pd.Series([0, 2.5, -1, -1])
        assert_series_equal(result, expected)

    def test_y(self):
        geoseries = sgpd.GeoSeries(
            [Point(0, -1, 2.5), Point(2.5, 0, -1), Point(-1, 2.5, 0), Point(-1, 0)]
        )
        result = geoseries.y.to_pandas()
        expected = pd.Series([-1, 0, 2.5, 0])
        assert_series_equal(result, expected)

    def test_z(self):
        geoseries = sgpd.GeoSeries(
            [Point(0, -1, 2.5), Point(2.5, 0, -1), Point(-1, 2.5, 0), Point(-1, 0)]
        )
        result = geoseries.z.to_pandas()
        expected = pd.Series([2.5, -1, 0, np.nan])
        assert_series_equal(result, expected)

    def test_m(self):
        pass

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
        assert_series_equal(result.to_pandas(), expected)

    @pytest.mark.parametrize("fun", ["notna", "notnull"])
    def test_notna(self, fun):
        geoseries = GeoSeries([Polygon([(0, 0), (1, 1), (0, 1)]), None, Polygon([])])
        result = getattr(geoseries, fun)()
        expected = pd.Series([True, False, True])
        assert_series_equal(result.to_pandas(), expected)

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

    def test_explode(self):
        pass

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

    def test_total_bounds(self):
        d = [
            Point(3, -1),
            Polygon([(0, 0), (1, 1), (1, 0)]),
            LineString([(0, 1), (1, 2)]),
            None,
        ]
        geoseries = sgpd.GeoSeries(d, crs="EPSG:4326")
        result = geoseries.total_bounds
        expected = np.array([0.0, -1.0, 3.0, 2.0])
        np.testing.assert_array_equal(result, expected)

        df_result = geoseries.to_geoframe().total_bounds
        np.testing.assert_array_equal(df_result, expected)

    # These tests were taken directly from the TestEstimateUtmCrs class in the geopandas test suite
    # https://github.com/geopandas/geopandas/blob/main/geopandas/tests/test_array.py
    def test_estimate_utm_crs(self):
        from pyproj import CRS

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
            ]
        )
        assert_series_equal(result.to_pandas(), expected)

        df_result = geoseries.to_geoframe().geom_type
        assert_series_equal(df_result.to_pandas(), expected)

    def test_type(self):
        pass

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
        result = geoseries.length.to_pandas()
        expected = pd.Series([0.000000, 1.414214, 3.414214, 4.828427])
        assert_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = geoseries.to_geoframe().length.to_pandas()
        assert_series_equal(df_result, expected)

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
        assert_series_equal(result.to_pandas(), expected)

        # Check that GeoDataFrame works too
        df_result = geoseries.to_geoframe().is_valid.to_pandas()
        assert_series_equal(df_result, expected)

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
        result = s.is_valid_reason().to_pandas()
        expected = pd.Series(
            [
                "Valid Geometry",
                "Self-intersection at or near point (0.5, 0.5, NaN)",
                "Valid Geometry",
                "Ring Self-intersection at or near point (1.0, 1.0)",
                None,
            ]
        )
        assert_series_equal(result, expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().is_valid_reason().to_pandas()
        assert_series_equal(df_result, expected)

    def test_is_empty(self):
        geoseries = sgpd.GeoSeries(
            [Point(), Point(2, 1), Polygon([(0, 0), (1, 1), (0, 1)]), None],
        )

        result = geoseries.is_empty
        expected = pd.Series([True, False, False, False])
        assert_series_equal(result.to_pandas(), expected)

        # Check that GeoDataFrame works too
        df_result = geoseries.to_geoframe().is_empty.to_pandas()
        assert_series_equal(df_result, expected)

    def test_count_coordinates(self):
        pass

    def test_count_geometries(self):
        pass

    def test_count_interior_rings(self):
        pass

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
        assert_series_equal(result.to_pandas(), expected)

        result = s.dwithin(s2, distance=1, align=True)
        expected = pd.Series([False, True, False, False, False])

        result = s.dwithin(s2, distance=1, align=False)
        expected = pd.Series([True, False, False, True])
        assert_series_equal(result.to_pandas(), expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().dwithin(s2, distance=1, align=False).to_pandas()
        assert_series_equal(df_result, expected)

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
        assert_series_equal(result.to_pandas(), expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().is_simple.to_pandas()
        assert_series_equal(df_result, expected)

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
        pass

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
        assert_series_equal(result.to_pandas(), expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().has_z.to_pandas()
        assert_series_equal(df_result, expected)

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
        pass

    def test_convex_hull(self):
        pass

    def test_delaunay_triangles(self):
        pass

    def test_voronoi_polygons(self):
        pass

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
        pass

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
        assert_series_equal(result.to_pandas(), expected)

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
        assert_series_equal(result.to_pandas(), expected)

        result = s.crosses(s2, align=True)
        expected = pd.Series([False, True, False, False, False])
        assert_series_equal(result.to_pandas(), expected)

        result = s.crosses(s2, align=False)
        expected = pd.Series([True, True, False, False])
        assert_series_equal(result.to_pandas(), expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().crosses(s2, align=False).to_pandas()
        assert_series_equal(df_result, expected)

    def test_disjoint(self):
        pass

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
        assert_series_equal(result.to_pandas(), expected)

        line = LineString([(-1, 1), (3, 1)])
        result = s.intersects(line)
        expected = pd.Series([True, True, True, True])
        assert_series_equal(result.to_pandas(), expected)

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
        df_result = s.to_geoframe().intersects(s2, align=False).to_pandas()
        assert_series_equal(df_result, expected)

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
        assert_series_equal(result.to_pandas(), expected)

        result = s.overlaps(s2, align=True)
        expected = pd.Series([False, True, False, False, False])
        assert_series_equal(result.to_pandas(), expected)

        result = s.overlaps(s2, align=False)
        expected = pd.Series([True, False, True, False])
        assert_series_equal(result.to_pandas(), expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().overlaps(s2, align=False)
        assert_series_equal(df_result.to_pandas(), expected)

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
        assert_series_equal(result.to_pandas(), expected)

        result = s.touches(s2, align=True)
        expected = pd.Series([False, True, True, False, False])
        assert_series_equal(result.to_pandas(), expected)

        result = s.touches(s2, align=False)
        expected = pd.Series([True, False, True, False])
        assert_series_equal(result.to_pandas(), expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().touches(s2, align=False)
        assert_series_equal(df_result.to_pandas(), expected)

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
        assert_series_equal(result.to_pandas(), expected)

        result = s2.within(s, align=True)
        expected = pd.Series([False, False, True, False, False])
        assert_series_equal(result.to_pandas(), expected)

        result = s2.within(s, align=False)
        expected = pd.Series([True, False, True, True], index=range(1, 5))
        assert_series_equal(result.to_pandas(), expected)

        # Ensure we return False if either geometries are empty
        s = GeoSeries([Point(), Point(), Polygon(), Point(0, 1)])
        result = s.within(s2, align=False)
        expected = pd.Series([False, False, False, True])
        assert_series_equal(result.to_pandas(), expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().within(s2, align=False)
        assert_series_equal(df_result.to_pandas(), expected)

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
        assert_series_equal(result.to_pandas(), expected)

        result = s.covers(s2, align=True)
        expected = pd.Series([False, False, False, False, False])
        assert_series_equal(result.to_pandas(), expected)

        result = s.covers(s2, align=False)
        expected = pd.Series([True, False, True, True])
        assert_series_equal(result.to_pandas(), expected)

        # Ensure we return False if either geometries are empty
        s = GeoSeries([Point(), Point(), Polygon(), Point(0, 0)])
        result = s.covers(s2, align=False)
        expected = pd.Series([False, False, False, True])
        assert_series_equal(result.to_pandas(), expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().covers(s2, align=False)
        assert_series_equal(df_result.to_pandas(), expected)

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
        assert_series_equal(result.to_pandas(), expected)

        result = s.covered_by(s2, align=True)
        expected = pd.Series([False, True, True, True, False])
        assert_series_equal(result.to_pandas(), expected)

        result = s.covered_by(s2, align=False)
        expected = pd.Series([True, False, True, True])
        assert_series_equal(result.to_pandas(), expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().covered_by(s2, align=False)
        assert_series_equal(df_result.to_pandas(), expected)

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
        assert_series_equal(result.to_pandas(), expected)

        result = s.distance(s2, align=True)
        expected = pd.Series([np.nan, 0.707107, 2.000000, 1.000000, np.nan])
        assert_series_equal(result.to_pandas(), expected)

        result = s.distance(s2, align=False)
        expected = pd.Series([0.000000, 3.162278, 0.707107, 1.000000])
        assert_series_equal(result.to_pandas(), expected)

        # Check that GeoDataFrame works too
        df_result = s.to_geoframe().distance(s2, align=False)
        assert_series_equal(df_result.to_pandas(), expected)

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

    def test_intersection_all(self):
        pass

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
        assert_series_equal(result.to_pandas(), expected)

        result = s2.contains(s, align=True)
        expected = pd.Series([False, False, False, True, False])
        assert_series_equal(result.to_pandas(), expected)

        result = s2.contains(s, align=False)
        expected = pd.Series([True, False, True, True], index=range(1, 5))
        assert_series_equal(result.to_pandas(), expected)

        # Check that GeoDataFrame works too
        df_result = s2.to_geoframe().contains(s, align=False)
        assert_series_equal(df_result.to_pandas(), expected)

    def test_contains_properly(self):
        pass

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

    def test_set_crs(self):
        geo_series = sgpd.GeoSeries([Point(0, 0), Point(1, 1)], name="geometry")
        assert geo_series.crs == None
        geo_series = geo_series.set_crs(epsg=4326)
        assert geo_series.crs.to_epsg() == 4326

        with pytest.raises(ValueError):
            geo_series.set_crs(4328, allow_override=False)
        with pytest.raises(ValueError):
            geo_series.set_crs(None, allow_override=False)

        geo_series = geo_series.set_crs(None, allow_override=True)
        assert geo_series.crs == None

        # Check that the name is preserved for set_crs
        geo_series.name = "geometry"

        geo_series.set_crs(4326, inplace=True)
        assert geo_series.crs.to_epsg() == 4326

        # Check that the name is preserved for set_crs after inplace=True
        geo_series.name = "geometry"

        geo_series = sgpd.GeoSeries(self.geoseries, crs=4326)
        assert geo_series.crs.to_epsg() == 4326

        # This test errors due to a bug in pyspark.
        # We can uncomment it once the fix is https://github.com/apache/spark/pull/51475 is merged
        # It was tested locally by using the fixed version of pyspark
        # # First element null
        # geo_series = sgpd.GeoSeries([None, None, Point(1, 1)], crs=4326)
        # assert geo_series.crs.to_epsg() == 4326
