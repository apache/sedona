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

import pytest

import numpy as np
import rasterio.features
from affine import Affine
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

    @pytest.mark.xfail(
        strict=True,
        raises=AssertionError,
        reason="allTouched samples ring segments at fixed 0.2-pixel steps "
        "(Rasterization.drawLineBresenham), so pixels the boundary clips with "
        "a chord shorter than the step fall between samples and are never "
        "burned where GDAL burns them. This strict xfail starts failing the "
        "day the sampling is replaced with exact cell traversal.",
    )
    def test_as_raster_all_touched_matches_gdal(self):
        """The same corpus under allTouched=true, kept as a strict expected
        failure so the known sampling divergence stays visible."""
        self._assert_matches_gdal(all_touched=True)
