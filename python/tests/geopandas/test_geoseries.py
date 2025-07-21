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
import sedona.geopandas as sgpd
from sedona.geopandas import GeoSeries
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

    def test_area(self):
        result = self.geoseries.area.to_pandas()
        expected = pd.Series([0.0, 0.0, 5.23, 5.23])
        assert result.count() > 0
        assert_series_equal(result, expected)

    def test_buffer(self):
        result = self.geoseries.buffer(1)
        expected = [
            "POLYGON ((3.300000000000000 -1.000000000000000, 3.280785280403230 -1.195090322016128, 3.223879532511287 -1.382683432365090, 3.131469612302545 -1.555570233019602, 3.007106781186547 -1.707106781186547, 2.855570233019602 -1.831469612302545, 2.682683432365089 -1.923879532511287, 2.495090322016128 -1.980785280403230, 2.300000000000000 -2.000000000000000, 2.104909677983872 -1.980785280403230, 1.917316567634910 -1.923879532511287, 1.744429766980398 -1.831469612302545, 1.592893218813452 -1.707106781186547, 1.468530387697454 -1.555570233019602, 1.376120467488713 -1.382683432365090, 1.319214719596769 -1.195090322016129, 1.300000000000000 -1.000000000000000, 1.319214719596769 -0.804909677983872, 1.376120467488713 -0.617316567634910, 1.468530387697454 -0.444429766980398, 1.592893218813452 -0.292893218813453, 1.744429766980398 -0.168530387697455, 1.917316567634910 -0.076120467488713, 2.104909677983871 -0.019214719596770, 2.300000000000000 0.000000000000000, 2.495090322016128 -0.019214719596770, 2.682683432365090 -0.076120467488713, 2.855570233019602 -0.168530387697455, 3.007106781186547 -0.292893218813452, 3.131469612302545 -0.444429766980398, 3.223879532511286 -0.617316567634910, 3.280785280403230 -0.804909677983871, 3.300000000000000 -1.000000000000000))",
            "POLYGON ((0.986393923832144 -3.164398987305357, 0.935367989801224 -3.353676015097457, 0.848396388482656 -3.529361471973156, 0.728821389740875 -3.684703864350261, 0.581238193719096 -3.813733471206735, 0.411318339874827 -3.911491757111723, 0.225591752899151 -3.974221925961374, 0.031195801372873 -3.999513292546280, -0.164398987305357 -3.986393923832144, -0.353676015097457 -3.935367989801224, -0.529361471973156 -3.848396388482656, -0.684703864350260 -3.728821389740875, -0.813733471206735 -3.581238193719097, -0.911491757111723 -3.411318339874827, -0.974221925961374 -3.225591752899151, -0.999513292546279 -3.031195801372874, -0.986393923832144 -2.835601012694643, -0.486393923832144 0.164398987305357, -0.435367989801224 0.353676015097458, -0.348396388482656 0.529361471973156, -0.228821389740875 0.684703864350260, -0.081238193719096 0.813733471206735, 0.088681660125173 0.911491757111723, 0.274408247100849 0.974221925961374, 0.468804198627127 0.999513292546279, 0.664398987305357 0.986393923832144, 0.853676015097457 0.935367989801224, 1.029361471973156 0.848396388482656, 1.184703864350260 0.728821389740875, 1.313733471206735 0.581238193719096, 1.411491757111723 0.411318339874827, 1.474221925961374 0.225591752899151, 1.499513292546280 0.031195801372873, 1.486393923832144 -0.164398987305357, 0.986393923832144 -3.164398987305357))",
            "POLYGON ((-0.260059926604056 -1.672672793996312, -0.403493516968407 -1.802608257932399, -0.569270104475049 -1.902480890158382, -0.751180291696993 -1.968549819451744, -0.942410374326119 -1.998340340272165, -1.135797558140999 -1.990736606370705, -1.324098251632999 -1.946023426395157, -1.500259385009482 -1.865875595977814, -1.657682592935656 -1.753295165887471, -1.790471365675451 -1.612498995956065, -1.893651911234561 -1.448760806607280, -1.963359455800552 -1.268213644171327, -1.996983004332570 -1.077620158927971, -1.993263139087243 -0.884119300439822, -1.293263139087243 5.115880699560178, -1.252729137381052 5.303820984767603, -1.176977926029782 5.480530662139786, -1.068809614934931 5.639477736894415, -0.932222597700009 5.774786800970082, -0.772265752785876 5.881456214877171, -0.594851813959648 5.955542991081357, -0.406538808715662 5.994308544787506, -0.214287643700274 5.996319924510972, -0.025204797887634 5.961502780493132, 0.153720365261017 5.891144113007211, 0.315873956515097 5.787844698964485, 0.455262040354176 5.655422955350244, 0.566732198133767 5.498773793134933, 0.646163984953356 5.323687679062990, 1.946163984953356 1.523687679062990, 1.993263731568509 1.315875621036525, 1.995265095723606 1.102802318781350, 1.952077207005038 0.894142203137658, 1.865660978573300 0.699369327572194, 1.739940073395944 0.527327206003688, -0.260059926604056 -1.672672793996312))",
            "POLYGON ((-0.844303230213814 -1.983056850984667, -0.942410374326119 -1.998340340272165, -1.135797558140999 -1.990736606370705, -1.324098251632999 -1.946023426395157, -1.500259385009482 -1.865875595977814, -1.657682592935656 -1.753295165887471, -1.790471365675451 -1.612498995956065, -1.893651911234561 -1.448760806607280, -1.963359455800552 -1.268213644171327, -1.996983004332570 -1.077620158927971, -1.993263139087243 -0.884119300439822, -1.293263139087243 5.115880699560178, -1.252729137381052 5.303820984767603, -1.176977926029782 5.480530662139786, -1.068809614934931 5.639477736894415, -0.932222597700009 5.774786800970082, -0.772265752785876 5.881456214877171, -0.594851813959648 5.955542991081357, -0.406538808715662 5.994308544787506, -0.214287643700274 5.996319924510972, -0.025204797887634 5.961502780493132, 0.153720365261017 5.891144113007211, 0.315873956515097 5.787844698964485, 0.455262040354176 5.655422955350244, 0.566732198133767 5.498773793134933, 0.646163984953356 5.323687679062990, 1.946163984953356 1.523687679062990, 1.993263731568509 1.315875621036525, 1.995265095723606 1.102802318781350, 1.952077207005038 0.894142203137658, 1.865660978573300 0.699369327572194, 1.739940073395944 0.527327206003688, 1.471895863976614 0.232478575642425, 1.474221925961374 0.225591752899151, 1.499513292546280 0.031195801372873, 1.486393923832144 -0.164398987305357, 1.426669391220515 -0.522746182975131, 1.468530387697454 -0.444429766980398, 1.592893218813452 -0.292893218813453, 1.744429766980398 -0.168530387697455, 1.917316567634910 -0.076120467488713, 2.104909677983871 -0.019214719596770, 2.300000000000000 0.000000000000000, 2.495090322016128 -0.019214719596770, 2.682683432365090 -0.076120467488713, 2.855570233019602 -0.168530387697455, 3.007106781186547 -0.292893218813452, 3.131469612302545 -0.444429766980398, 3.223879532511286 -0.617316567634910, 3.280785280403230 -0.804909677983871, 3.300000000000000 -1.000000000000000, 3.280785280403230 -1.195090322016128, 3.223879532511287 -1.382683432365090, 3.131469612302545 -1.555570233019602, 3.007106781186547 -1.707106781186547, 2.855570233019602 -1.831469612302545, 2.682683432365089 -1.923879532511287, 2.495090322016128 -1.980785280403230, 2.300000000000000 -2.000000000000000, 2.104909677983872 -1.980785280403230, 1.917316567634910 -1.923879532511287, 1.744429766980398 -1.831469612302545, 1.592893218813452 -1.707106781186547, 1.468530387697454 -1.555570233019602, 1.376120467488713 -1.382683432365090, 1.319214719596769 -1.195090322016129, 1.317505079406277 -1.177732053860557, 0.986393923832144 -3.164398987305357, 0.935367989801224 -3.353676015097457, 0.848396388482656 -3.529361471973156, 0.728821389740875 -3.684703864350261, 0.581238193719096 -3.813733471206735, 0.411318339874827 -3.911491757111723, 0.225591752899151 -3.974221925961374, 0.031195801372873 -3.999513292546280, -0.164398987305357 -3.986393923832144, -0.353676015097457 -3.935367989801224, -0.529361471973156 -3.848396388482656, -0.684703864350260 -3.728821389740875, -0.813733471206735 -3.581238193719097, -0.911491757111723 -3.411318339874827, -0.974221925961374 -3.225591752899151, -0.999513292546279 -3.031195801372874, -0.986393923832144 -2.835601012694643, -0.844303230213814 -1.983056850984667))",
        ]
        expected = gpd.GeoSeries([wkt.loads(wkt_str) for wkt_str in expected])
        assert result.count() > 0
        self.check_sgpd_equals_gpd(result, expected)

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
        pass

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

        data = [Point(0, 0), None]
        # Ensure filling with np.nan or pd.NA returns None
        import numpy as np

        for fill_val in [np.nan, pd.NA]:
            result = GeoSeries(data).fillna(fill_val)
            expected = gpd.GeoSeries([Point(0, 0), None])
            self.check_sgpd_equals_gpd(result, expected)

        # Ensure filling with None is empty GeometryColleciton and not None
        # Also check that inplace works
        result = GeoSeries(data)
        result.fillna(None, inplace=True)
        expected = gpd.GeoSeries([Point(0, 0), GeometryCollection()])
        self.check_sgpd_equals_gpd(result, expected)

    def test_explode(self):
        pass

    def test_to_crs(self):
        from pyproj import CRS

        geoseries = sgpd.GeoSeries([Point(1, 1), Point(2, 2), Point(3, 3)], crs=4326)
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
        pass

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
        pass

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
                # Errors for LinearRing: issue #2120
                # LinearRing([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)]),
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
                # "LineString",  # Note: Sedona returns LineString instead of LinearRing
            ]
        )
        assert_series_equal(result.to_pandas(), expected)

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

    def test_is_empty(self):
        geoseries = sgpd.GeoSeries(
            [Point(), Point(2, 1), Polygon([(0, 0), (1, 1), (0, 1)]), None],
        )

        result = geoseries.is_empty
        expected = pd.Series([True, False, False, False])
        assert_series_equal(result.to_pandas(), expected)

    def test_count_coordinates(self):
        pass

    def test_count_geometries(self):
        pass

    def test_count_interior_rings(self):
        pass

    def test_is_simple(self):
        s = sgpd.GeoSeries(
            [
                LineString([(0, 0), (1, 1), (1, -1), (0, 1)]),
                LineString([(0, 0), (1, 1), (1, -1)]),
                # Errors for LinearRing: issue #2120
                # LinearRing([(0, 0), (1, 1), (1, -1), (0, 1)]),
                # LinearRing([(0, 0), (-1, 1), (-1, -1), (1, -1)]),
            ]
        )
        result = s.is_simple
        expected = pd.Series([False, True])
        # Removed LinearRing cases: False, True]
        assert_series_equal(result.to_pandas(), expected)

    def test_is_ring(self):
        pass

    def test_is_ccw(self):
        pass

    def test_is_closed(self):
        pass

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

        result = GeoSeries(
            [Polygon([(0, 0), (1, 1), (1, 2), (1, 1), (0, 0)])]
        ).make_valid(method="structure", keep_collapsed=True)
        expected = gpd.GeoSeries([LineString([(0, 0), (1, 1), (1, 2), (1, 1), (0, 0)])])
        self.check_sgpd_equals_gpd(result, expected)

        result = GeoSeries(
            [Polygon([(0, 0), (1, 1), (1, 2), (1, 1), (0, 0)])]
        ).make_valid(method="structure", keep_collapsed=False)
        expected = gpd.GeoSeries([Polygon()])
        self.check_sgpd_equals_gpd(result, expected)

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
        s = GeoSeries([box(0, 0, 1, 1), box(0, 0, 2, 2)])
        result = s.union_all()
        expected = Polygon([(0, 1), (0, 2), (2, 2), (2, 0), (1, 0), (0, 0), (0, 1)])
        self.check_geom_equals(result, expected)

        # Empty GeoSeries
        s = sgpd.GeoSeries([])
        result = s.union_all()
        expected = GeometryCollection()
        self.check_geom_equals(result, expected)

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

    def test_intersection(self):
        import pyspark.pandas as ps

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
        with self.ps_allow_diff_frames():
            assert s2.index.equals(expected_index)

        result = s.intersection(s2, align=True)
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

        result = s2.intersection(s, align=False)
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

    def test_contains_properly(self):
        pass

    def test_set_crs(self):
        geo_series = sgpd.GeoSeries(self.geoseries)
        assert geo_series.crs == None
        geo_series = geo_series.set_crs(epsg=4326)
        assert geo_series.crs.to_epsg() == 4326

        with pytest.raises(ValueError):
            geo_series.set_crs(4328)
        with pytest.raises(ValueError):
            geo_series.crs = None

        geo_series = geo_series.set_crs(None, allow_override=True)
        assert geo_series.crs == None

        geo_series.set_crs(4326, inplace=True)
        assert geo_series.crs.to_epsg() == 4326

        geo_series = sgpd.GeoSeries(self.geoseries, crs=4326)
        assert geo_series.crs.to_epsg() == 4326
