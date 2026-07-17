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
from pyspark.sql.functions import expr
from shapely.geometry import box
from shapely.wkt import loads as wkt_loads

from tests.test_base import TestBase

# (burn value, nodata value) pairs; value = 0 with a nonzero nodata is the
# case where a post-hoc background fill could not tell burned pixels apart.
VALUE_NODATA_PAIRS = [(1.0, 9.0), (0.0, 9.0), (7.0, 255.0), (1.0, 100.0)]


def _nodata_cases(count=60, seed=31114):
    """Seeded random rectilinear polygons over anisotropic grids.

    All rings are axis-aligned (boxes, L-shapes, boxes with box holes), so
    the pixel selection is identical across rasterizers and any divergence
    from the GDAL reference is purely the background fill. The fixed seed
    makes the corpus deterministic, so a failure reproduces from the case id
    alone.
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

        def sub_box(fx0, fy0, fx1, fy1):
            return box(
                xs[0] + (xs[1] - xs[0]) * fx0,
                ys[0] + (ys[1] - ys[0]) * fy0,
                xs[0] + (xs[1] - xs[0]) * fx1,
                ys[0] + (ys[1] - ys[0]) * fy1,
            )

        fx0, fy0 = rng.uniform(0.05, 0.4), rng.uniform(0.05, 0.4)
        fx1, fy1 = rng.uniform(fx0 + 0.3, 0.95), rng.uniform(fy0 + 0.3, 0.95)
        outer = sub_box(fx0, fy0, fx1, fy1)
        kind = rng.choice(["box", "hole", "ell"])
        if kind == "hole":
            hx0 = fx0 + (fx1 - fx0) * rng.uniform(0.2, 0.35)
            hy0 = fy0 + (fy1 - fy0) * rng.uniform(0.2, 0.35)
            hx1 = fx0 + (fx1 - fx0) * rng.uniform(0.6, 0.8)
            hy1 = fy0 + (fy1 - fy0) * rng.uniform(0.6, 0.8)
            geom = outer.difference(sub_box(hx0, hy0, hx1, hy1))
        elif kind == "ell":
            # The second box starts inside the outer one and pokes past its
            # right edge over a sub-range of its height, so the union is a
            # single axis-aligned polygon with a concave corner.
            geom = outer.union(
                sub_box(
                    fx0 + (fx1 - fx0) * rng.uniform(0.3, 0.6),
                    fy0 + (fy1 - fy0) * rng.uniform(0.1, 0.3),
                    min(fx1 + rng.uniform(0.02, 0.04), 0.98),
                    fy0 + (fy1 - fy0) * rng.uniform(0.5, 0.8),
                )
            )
        else:
            geom = outer
        if geom.geom_type != "Polygon":
            continue

        value, nodata = VALUE_NODATA_PAIRS[len(cases) % len(VALUE_NODATA_PAIRS)]
        cases.append(
            (
                len(cases),
                width,
                height,
                upper_left_x,
                upper_left_y,
                scale_x,
                scale_y,
                value,
                nodata,
                geom.wkt,
            )
        )
    return cases


class TestRasterizeNoData(TestBase):
    def test_as_raster_background_is_nodata(self):
        """Pixels not covered by the geometry (outside it, or inside a hole)
        must read back as the declared noDataValue, matching GDAL
        (rasterio.features.rasterize with fill=nodata) and the band's nodata
        metadata. Rings are axis-aligned so pixel selection cannot differ
        between rasterizers; only the fill policy is under test.
        """
        cases = _nodata_cases()
        df = self.spark.createDataFrame(
            cases,
            "id INT, width INT, height INT, ulx DOUBLE, uly DOUBLE, "
            "sx DOUBLE, sy DOUBLE, value DOUBLE, nodata DOUBLE, wkt STRING",
        )
        rows = (
            df.withColumn(
                "r",
                expr(
                    "RS_AsRaster(ST_GeomFromWKT(wkt), "
                    "RS_MakeEmptyRaster(1, 'd', width, height, ulx, uly, sx, sy, 0, 0, 0), "
                    "'d', false, value, nodata, false)"
                ),
            )
            .selectExpr(
                "id",
                "RS_BandAsArray(r, 1) as band",
                "RS_BandNoDataValue(r, 1) as nd",
            )
            .collect()
        )
        results = {row["id"]: row for row in rows}
        assert len(results) == len(cases)

        for case_id, width, height, ulx, uly, sx, sy, value, nodata, wkt in cases:
            row = results[case_id]
            assert row["nd"] == nodata, f"case {case_id}: nodata metadata"
            actual = np.array(row["band"], dtype=float).reshape(height, width)
            expected = rasterio.features.rasterize(
                [(wkt_loads(wkt), value)],
                out_shape=(height, width),
                fill=nodata,
                transform=Affine(sx, 0, ulx, 0, sy, uly),
                all_touched=False,
                dtype="float64",
            )
            assert np.array_equal(actual, expected), (
                f"case {case_id}: grid {width}x{height}, "
                f"scale ({sx}, {sy}), value {value}, nodata {nodata}, {wkt}"
            )
