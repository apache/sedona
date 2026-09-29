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

import random

import numpy as np
import rasterio.features
from affine import Affine
from shapely.affinity import affine_transform
from shapely.geometry import Polygon
from shapely.wkt import loads as wkt_loads

from tests.test_base import TestBase


def _rasterize_cases(count=100, seed=31113):
    """Seeded random polygons over anisotropic north-up and south-up grids.

    The fixed seed makes the corpus deterministic, so a failure is
    reproducible from the case id alone. Pixel width and height are drawn
    independently, so the grids exercise the non-square pixel aspect ratios
    where scanline arithmetic errors hide (square unit grids make pixel-space
    and world-space slopes coincide).
    """
    rng = random.Random(seed)
    cases = []
    while len(cases) < count:
        width, height = rng.randint(4, 12), rng.randint(4, 12)
        scale_x = round(rng.uniform(0.3, 5.0), 3)
        scale_y = round(rng.uniform(0.3, 5.0), 3) * rng.choice([-1, 1])
        upper_left_x = round(rng.uniform(-1000, 1000), 2)
        upper_left_y = round(rng.uniform(-1000, 1000), 2)
        xs = sorted([upper_left_x, upper_left_x + width * scale_x])
        ys = sorted([upper_left_y, upper_left_y + height * scale_y])
        margin_x = (xs[1] - xs[0]) * 0.05
        margin_y = (ys[1] - ys[0]) * 0.05

        def random_point():
            return (
                round(rng.uniform(xs[0] + margin_x, xs[1] - margin_x), 3),
                round(rng.uniform(ys[0] + margin_y, ys[1] - margin_y), 3),
            )

        num_points = rng.choice([3, 3, 4, 5])
        for _ in range(50):
            candidate = Polygon([random_point() for _ in range(num_points)]).buffer(0)
            grid_area = (xs[1] - xs[0]) * (ys[1] - ys[0])
            if candidate.geom_type == "Polygon" and candidate.area > grid_area * 0.02:
                cases.append(
                    (
                        len(cases),
                        width,
                        height,
                        upper_left_x,
                        upper_left_y,
                        scale_x,
                        scale_y,
                        candidate.wkt,
                    )
                )
                break
    return cases


class TestRasterizeParity(TestBase):
    def _assert_matches_gdal(self, all_touched):
        cases = _rasterize_cases()
        df = self.spark.createDataFrame(
            cases,
            "id INT, width INT, height INT, ulx DOUBLE, uly DOUBLE, "
            "sx DOUBLE, sy DOUBLE, wkt STRING",
        )
        rows = df.selectExpr(
            "id",
            "RS_BandAsArray(RS_AsRaster(ST_GeomFromWKT(wkt), "
            "RS_MakeEmptyRaster(1, 'd', width, height, ulx, uly, sx, sy, 0, 0, 0), "
            f"'d', {str(all_touched).lower()}, 1.0, 0.0, false), 1) as band",
        ).collect()
        bands = {row["id"]: row["band"] for row in rows}
        assert len(bands) == len(cases)

        for case_id, width, height, ulx, uly, sx, sy, wkt in cases:
            actual = np.array(bands[case_id], dtype=float).reshape(height, width)
            expected = rasterio.features.rasterize(
                [(wkt_loads(wkt), 1)],
                out_shape=(height, width),
                fill=0,
                transform=Affine(sx, 0, ulx, 0, sy, uly),
                all_touched=all_touched,
                dtype="uint8",
            )
            assert np.array_equal(actual, expected), (
                f"case {case_id}: grid {width}x{height}, "
                f"scale ({sx}, {sy}), origin ({ulx}, {uly}), {wkt}"
            )

    def test_as_raster_centroid_rule_matches_gdal(self):
        """RS_AsRaster with allTouched=false must burn exactly the pixels whose
        centers fall inside the polygon, matching GDAL
        (rasterio.features.rasterize) on the same grid.
        """
        self._assert_matches_gdal(all_touched=False)

    def test_as_raster_all_touched_matches_gdal(self):
        """The same corpus under allTouched=true. Line/boundary segments are
        rasterized by exact cell traversal, so every pixel the boundary touches
        is burned, matching GDAL."""
        self._assert_matches_gdal(all_touched=True)

    def test_as_raster_linestring_matches_gdal(self):
        """A line crossing a 6x6 unit grid: every pixel the line touches must
        be burned, matching GDAL. Independent of allTouched (a line has no
        interior) and of #3111 (square unit pixels)."""
        wkt = "LINESTRING (3.97 1.57, 0.31 3.24)"
        band = self.spark.sql(
            "SELECT RS_BandAsArray(RS_AsRaster(ST_GeomFromWKT('" + wkt + "'), "
            "RS_MakeEmptyRaster(1, 'd', 6, 6, 0.0, 6.0, 1.0, -1.0, 0.0, 0.0, 0), "
            "'d', false, 1.0, 0.0, false), 1)"
        ).first()[0]
        actual = np.array(band, dtype=float).reshape(6, 6)
        expected = rasterio.features.rasterize(
            [(wkt_loads(wkt), 1)],
            out_shape=(6, 6),
            fill=0,
            transform=Affine(1, 0, 0, 0, -1, 6),
            all_touched=True,
            dtype="uint8",
        )
        assert np.array_equal(actual, expected)

    def test_as_raster_grid_aligned_vertices_match_gdal(self):
        """Vertices that land exactly on grid lines (GH-3120): a cell the
        geometry touches only at such a vertex point is not burned, matching
        GDAL. Cases where a segment passes exactly through a lattice corner
        mid-segment are excluded: there GDAL burns one extra off-diagonal cell
        chosen by its internal scan order, which is slope-dependent and has no
        consistent reference (see the Java RasterizationTest for Sedona's
        direction-independent behaviour at those corners)."""
        cases = [
            # id, width, height, ulx, uly, sx, sy, wkt
            # endpoint exactly on a horizontal grid line, arriving and leaving
            (0, 6, 6, 0.0, 6.0, 1.0, -1.0, "LINESTRING (1.5 4.5, 3.8 3.0)"),
            (1, 6, 6, 0.0, 6.0, 1.0, -1.0, "LINESTRING (3.8 3.0, 1.5 4.5)"),
            (2, 6, 6, 0.0, 6.0, 1.0, -1.0, "LINESTRING (3.8 3.0, 4.5 1.6)"),
            # endpoint exactly on a vertical grid line
            (3, 6, 6, 0.0, 6.0, 1.0, -1.0, "LINESTRING (1.5 1.5, 3.0 2.5)"),
            (4, 6, 6, 0.0, 6.0, 1.0, -1.0, "LINESTRING (3.0 2.5, 4.5 1.5)"),
            # V whose apex vertex lies exactly on a lattice corner
            (5, 6, 6, 0.0, 6.0, 1.0, -1.0, "LINESTRING (2.5 2.5, 3.0 3.0, 3.5 2.5)"),
            # segment inside one cell ending on the cell's corner
            (6, 6, 6, 0.0, 6.0, 1.0, -1.0, "LINESTRING (2.2 2.2, 3.0 3.0)"),
            # segments lying exactly along a grid line keep the half-open side
            (7, 6, 6, 0.0, 6.0, 1.0, -1.0, "LINESTRING (1.5 3.0, 4.5 3.0)"),
            (8, 6, 6, 0.0, 6.0, 1.0, -1.0, "LINESTRING (3.0 1.5, 3.0 4.5)"),
        ]
        df = self.spark.createDataFrame(
            cases,
            "id INT, width INT, height INT, ulx DOUBLE, uly DOUBLE, "
            "sx DOUBLE, sy DOUBLE, wkt STRING",
        )
        rows = df.selectExpr(
            "id",
            "RS_BandAsArray(RS_AsRaster(ST_GeomFromWKT(wkt), "
            "RS_MakeEmptyRaster(1, 'd', width, height, ulx, uly, sx, sy, 0, 0, 0), "
            "'d', true, 1.0, 0.0, false), 1) as band",
        ).collect()
        bands = {row["id"]: row["band"] for row in rows}

        for case_id, width, height, ulx, uly, sx, sy, wkt in cases:
            actual = np.array(bands[case_id], dtype=float).reshape(height, width)
            expected = rasterio.features.rasterize(
                [(wkt_loads(wkt), 1)],
                out_shape=(height, width),
                fill=0,
                transform=Affine(sx, 0, ulx, 0, sy, uly),
                all_touched=True,
                dtype="uint8",
            )
            assert np.array_equal(actual, expected), f"case {case_id}: {wkt}"

    def test_as_raster_multipolygon_corner_apexes_match_gdal(self):
        """MultiPolygon whose diamond apex vertices land exactly on pixel
        corners (GH-3120): the cells beyond the apexes are touched only at
        those points and are not burned, matching GDAL. The pre-GH-3120
        traversal also ran off the raster past such corner endpoints, burning
        a diagonal streak of unrelated pixels."""
        wkt = (
            "MULTIPOLYGON (((2 -2, 4 -2, 4 -4, 2 -4, 2 -2)), "
            "((4 -4, 6 -4, 6 -6, 5 -7, 4 -6, 4 -4)), "
            "((6 -6, 8 -6, 8 -8, 6 -8, 6 -6)), "
            "((8 -6, 10 -6, 10 -4, 9 -3, 8 -4, 8 -6)))"
        )
        band = self.spark.sql(
            "SELECT RS_BandAsArray(RS_AsRaster(ST_GeomFromWKT('" + wkt + "'), "
            "RS_MakeEmptyRaster(1, 'd', 5, 4, 1.0, -1.0, 2.0, -2.0, 0.0, 0.0, 0), "
            "'d', true, 1.0, 0.0, false), 1)"
        ).first()[0]
        actual = np.array(band, dtype=float).reshape(4, 5)
        expected = rasterio.features.rasterize(
            [(wkt_loads(wkt), 1)],
            out_shape=(4, 5),
            fill=0,
            transform=Affine(2, 0, 1, 0, -2, -1),
            all_touched=True,
            dtype="uint8",
        )
        assert np.array_equal(actual, expected)

    def test_as_raster_half_open_cell_boundaries_match_gdal(self):
        """Geometries lying exactly on grid lines (GH-3425). GDAL treats every
        cell as half-open, with or without all_touched: a cell contains its
        first edge in pixel order but not its last. A point on a grid line or
        a grid corner burns exactly one cell, a point on the raster's far edge
        burns none, a grid-aligned polygon burns only its interior, and a
        polygon that only touches the raster burns nothing."""
        grids = [
            # width, height, ulx, uly, sx, sy
            (8, 8, 0.0, 8.0, 1.0, -1.0),  # north-up
            (8, 8, 0.0, 0.0, 1.0, 1.0),  # bottom-up
            (8, 8, 100.25, 50.5, 0.25, -0.25),  # decimal pixel size
        ]
        # Geometries in pixel coordinates (column, row), mapped onto each grid.
        pixel_wkts = [
            "POINT (3.5 3)",  # on a row line
            "POINT (3 3.5)",  # on a column line
            "POINT (3 3)",  # on a grid corner
            "POINT (0 3.5)",  # on the first column edge
            "POINT (8 3.5)",  # on the last column edge
            "POINT (3.5 0)",  # on the first row edge
            "POINT (3.5 8)",  # on the last row edge
            "POINT (8 8)",  # on the far corner
            "MULTIPOINT ((3 3.5), (5.5 5.5))",
            "POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2))",
            "POLYGON ((2 2, 6 2, 6 4, 4 4, 4 6, 2 6, 2 2))",
            "POLYGON ((1 1, 7 1, 7 7, 1 7, 1 1), (3 3, 5 3, 5 5, 3 5, 3 3))",
            "POLYGON ((-3 2, 2 2, 2 4, -3 4, -3 2))",  # past the first column edge
            "POLYGON ((6 2, 11 2, 11 4, 6 4, 6 2))",  # past the last column edge
            "POLYGON ((2 -3, 4 -3, 4 2, 2 2, 2 -3))",  # past the first row edge
            "POLYGON ((2 6, 4 6, 4 11, 2 11, 2 6))",  # past the last row edge
            "POLYGON ((-2 2, 0 2, 0 4, -2 4, -2 2))",  # touching the first column edge
            "POLYGON ((8 2, 10 2, 10 4, 8 4, 8 2))",  # touching the last column edge
            "POLYGON ((2 -2, 4 -2, 4 0, 2 0, 2 -2))",  # touching the first row edge
            "POLYGON ((2 8, 4 8, 4 10, 2 10, 2 8))",  # touching the last row edge
            "POLYGON ((8 8, 10 8, 10 10, 8 10, 8 8))",  # touching the far corner
        ]
        cases = []
        for width, height, ulx, uly, sx, sy in grids:
            for pixel_wkt in pixel_wkts:
                geom = affine_transform(wkt_loads(pixel_wkt), [sx, 0, 0, sy, ulx, uly])
                cases.append((len(cases), width, height, ulx, uly, sx, sy, geom.wkt))
        df = self.spark.createDataFrame(
            cases,
            "id INT, width INT, height INT, ulx DOUBLE, uly DOUBLE, "
            "sx DOUBLE, sy DOUBLE, wkt STRING",
        )
        for all_touched in (False, True):
            rows = df.selectExpr(
                "id",
                "RS_BandAsArray(RS_AsRaster(ST_GeomFromWKT(wkt), "
                "RS_MakeEmptyRaster(1, 'd', width, height, ulx, uly, sx, sy, 0, 0, 0), "
                f"'d', {str(all_touched).lower()}, 1.0, 0.0, false), 1) as band",
            ).collect()
            bands = {row["id"]: row["band"] for row in rows}

            for case_id, width, height, ulx, uly, sx, sy, wkt in cases:
                actual = np.array(bands[case_id], dtype=float).reshape(height, width)
                expected = rasterio.features.rasterize(
                    [(wkt_loads(wkt), 1)],
                    out_shape=(height, width),
                    fill=0,
                    transform=Affine(sx, 0, ulx, 0, sy, uly),
                    all_touched=all_touched,
                    dtype="uint8",
                )
                assert np.array_equal(actual, expected), (
                    f"case {case_id} all_touched={all_touched}: grid {width}x{height}, "
                    f"scale ({sx}, {sy}), origin ({ulx}, {uly}), {wkt}"
                )
