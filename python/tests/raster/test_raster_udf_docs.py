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
"""Executable copies of every example on docs/api/sql/Raster-UDF.md.

Each test mirrors one code block from that page so the documentation cannot
drift from what actually runs. When an example changes there, change it here.
"""

import math

import numpy as np
import pyspark
import pytest
import rasterio.fill
from pyspark.sql.functions import col, expr, udf
from pyspark.sql.types import DoubleType
from tests.test_base import TestBase

from sedona.spark.sql.types import RasterType

# A 4-band raster standing in for the doc's "multi-band scene". Band k holds
# k + y * 4 + x, so band 0 runs 0..11 and band 3 runs 3..14.
FOUR_BAND = (
    "RS_MakeRasterForTesting(4, 'D', 'BandedSampleModel', "
    "4, 3, 100, 100, 10, -10, 0, 0, 3857)"
)

requires_spark_34 = pytest.mark.skipif(
    pyspark.__version__ < "3.4", reason="requires Spark 3.4 or higher"
)


class TestRasterUdfDocExamples(TestBase):

    def _four_band_df(self):
        return self.spark.range(1).withColumn("rast", expr(FOUR_BAND))

    # ---- "Raster to scalar" ----------------------------------------------

    @requires_spark_34
    def test_raster_to_scalar(self):
        df = self._four_band_df()

        @udf(returnType="double")
        def mean_udf(raster):
            return float(raster.as_numpy().mean())

        result = df.select(mean_udf(col("rast")).alias("mean")).first()["mean"]
        # Band means are 5.5, 6.5, 7.5, 8.5.
        assert result == pytest.approx(7.0)

    @requires_spark_34
    def test_raster_to_scalar_registered_for_sql(self):
        """The register-by-name form: pass the decorated UDF, no returnType."""
        sedona = self.spark
        self._four_band_df().createOrReplaceTempView("raster_table")

        @udf(returnType=DoubleType())
        def mean_udf(raster):
            return float(raster.as_numpy().mean())

        sedona.udf.register("mean_udf", mean_udf)
        result = sedona.sql("SELECT mean_udf(rast) AS mean FROM raster_table").first()
        assert result["mean"] == pytest.approx(7.0)

    # ---- "Raster to raster" ----------------------------------------------

    @requires_spark_34
    def test_raster_to_raster(self):
        df = self._four_band_df()

        @udf(returnType=RasterType())
        def mask_udf(raster):
            band1 = raster.as_numpy()[0]
            mask = (band1 < 1400).astype(np.float32)
            return raster.with_bands(mask)

        result = (
            df.select(mask_udf(col("rast")).alias("mask_rast"))
            .selectExpr(
                "RS_NumBands(mask_rast) AS num_bands",
                "RS_BandAsArray(mask_rast, 1) AS band",
                "RS_SRID(mask_rast) AS srid",
            )
            .first()
        )
        assert result["num_bands"] == 1
        assert result["srid"] == 3857
        # Every band-0 value is below 1400, so the whole mask is set.
        assert all(value == 1.0 for value in result["band"])

    @requires_spark_34
    def test_raster_to_raster_with_nodata(self):
        """The nodata= form from 'Setting NODATA on the output'."""
        df = self._four_band_df()

        @udf(returnType=RasterType())
        def mask_udf(raster):
            band1 = raster.as_numpy()[0]
            mask = (band1 < 1400).astype(np.float32)
            return raster.with_bands(mask, nodata=-9999.0)

        result = (
            df.select(mask_udf(col("rast")).alias("mask_rast"))
            .selectExpr("RS_BandNoDataValue(mask_rast, 1) AS nodata")
            .first()
        )
        assert result["nodata"] == -9999.0

    @requires_spark_34
    def test_per_band_nodata_sequence(self):
        """The nodata=[...] form, including float('nan') for 'no NODATA'."""
        df = self._four_band_df()

        @udf(returnType=RasterType())
        def two_bands(raster):
            arr = raster.as_numpy().astype(np.float64)
            stacked = np.concatenate([arr[0:1], arr[1:2]], axis=0)
            return raster.with_bands(stacked, nodata=[-9999.0, float("nan")])

        result = (
            df.select(two_bands(col("rast")).alias("out"))
            .selectExpr(
                "RS_BandNoDataValue(out, 1) AS nodata1",
                "RS_BandNoDataValue(out, 2) AS nodata2",
            )
            .first()
        )
        assert result["nodata1"] == -9999.0
        assert result["nodata2"] is None

    # ---- "NDVI, as map algebra and as a UDF" ------------------------------

    @requires_spark_34
    def test_ndvi_udf_matches_map_algebra(self):
        """Both NDVI forms on the page must produce the same raster."""
        df = self._four_band_df()

        @udf(returnType=RasterType())
        def ndvi(raster):
            a = raster.as_numpy().astype(np.float64)
            red, nir = a[0], a[3]
            return raster.with_bands((nir - red) / (nir + red + 1e-10))

        from_udf = (
            df.select(ndvi(col("rast")).alias("ndvi"))
            .selectExpr("RS_BandAsArray(ndvi, 1) AS band")
            .first()["band"]
        )
        from_map_algebra = df.selectExpr(
            "RS_BandAsArray("
            "  RS_MapAlgebra(rast, 'D', "
            "                'out = (rast[3] - rast[0]) / (rast[3] + rast[0]);'), 1"
            ") AS band"
        ).first()["band"]

        assert len(from_udf) == len(from_map_algebra) == 12
        for got, want in zip(from_udf, from_map_algebra):
            assert math.isfinite(got)
            # The UDF adds 1e-10 to the denominator to guard against 0/0, which
            # the Jiffle script does not; that is the only difference.
            assert got == pytest.approx(want, abs=1e-6)

    # ---- "Two rasters" ----------------------------------------------------

    @requires_spark_34
    def test_two_raster_udf(self):
        @udf(returnType=RasterType())
        def plus_five(raster):
            return raster.with_bands(raster.as_numpy().astype(np.float64) + 5.0)

        df = (
            self.spark.range(1)
            .withColumn("before", expr(FOUR_BAND))
            .withColumn("after", plus_five(col("before")))
        )

        @udf(returnType=RasterType())
        def delta(after, before):
            diff = after.as_numpy()[0] - before.as_numpy()[0]
            return after.with_bands(diff)

        result = (
            df.select(delta(col("after"), col("before")).alias("delta"))
            .selectExpr(
                "RS_NumBands(delta) AS num_bands",
                "RS_BandAsArray(delta, 1) AS band",
            )
            .first()
        )
        assert result["num_bands"] == 1
        assert all(value == pytest.approx(5.0) for value in result["band"])

    # ---- "Using rasterio inside a UDF" ------------------------------------

    @requires_spark_34
    def test_rasterio_inside_udf(self):
        df = self._four_band_df()

        @udf(returnType=RasterType())
        def fill_udf(raster):
            with raster.as_rasterio() as src:
                filled = rasterio.fill.fillnodata(src.read(1), mask=src.read_masks(1))
            return raster.with_bands(filled)

        result = (
            df.select(fill_udf(col("rast")).alias("filled"))
            .selectExpr(
                "RS_NumBands(filled) AS num_bands",
                "RS_BandAsArray(filled, 1) AS band",
                "RS_SRID(filled) AS srid",
                "RS_ScaleX(filled) AS scale_x",
            )
            .first()
        )
        assert result["num_bands"] == 1
        assert result["srid"] == 3857
        assert result["scale_x"] == pytest.approx(10.0)
        # Nothing is masked, so fillnodata returns band 1 unchanged: 0..11.
        assert list(result["band"]) == [float(v) for v in range(12)]

    # ---- "Limits" ---------------------------------------------------------

    def test_output_must_match_input_grid(self):
        """The ValueError quoted under 'The output must sit on the input's grid'."""
        raster = self.spark.sql(f"SELECT {FOUR_BAND} AS rast").first()["rast"]
        with pytest.raises(
            ValueError, match=r"Spatial dimensions \(2, 2\) don't match"
        ):
            raster.with_bands(np.zeros((2, 2), dtype=np.float64))

    def test_unsupported_dtypes_are_rejected(self):
        """int64 and uint64 are rejected, per the dtype table."""
        raster = self.spark.sql(f"SELECT {FOUR_BAND} AS rast").first()["rast"]
        for dtype in (np.int64, np.uint64):
            with pytest.raises(ValueError, match="Unsupported numpy dtype"):
                raster.with_bands(np.zeros((3, 4), dtype=dtype))

    def test_supported_dtypes_are_accepted(self):
        raster = self.spark.sql(f"SELECT {FOUR_BAND} AS rast").first()["rast"]
        for dtype in (
            np.uint8,
            np.int8,
            np.int16,
            np.uint16,
            np.int32,
            np.uint32,
            np.float32,
            np.float64,
        ):
            out = raster.with_bands(np.zeros((3, 4), dtype=dtype))
            assert out.as_numpy().dtype == np.dtype(dtype)

    @requires_spark_34
    def test_int8_negative_values_are_reinterpreted(self):
        """The dtype table's claim that int8 -2 reads back as 254."""
        df = self._four_band_df()

        @udf(returnType=RasterType())
        def negative_int8(raster):
            arr = np.full((raster.height, raster.width), -2, dtype=np.int8)
            return raster.with_bands(arr)

        band = (
            df.select(negative_int8(col("rast")).alias("out"))
            .selectExpr("RS_BandAsArray(out, 1) AS band")
            .first()["band"]
        )
        assert all(value == 254.0 for value in band)
