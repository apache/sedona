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

        NODATA = -9999.0

        @udf(returnType=RasterType())
        def delta(after, before):
            diff = after.as_numpy_masked()[0] - before.as_numpy_masked()[0]
            return after.with_bands(
                np.where(np.isnan(diff), NODATA, diff), nodata=NODATA
            )

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

    @requires_spark_34
    def test_two_raster_udf_keeps_holes_invalid(self):
        """A hole in either input must stay a hole, not become a plausible number.

        Guards the trap that as_numpy() would fall into: subtracting raw sentinels
        turns a one-sided hole into a large bogus delta and a two-sided hole into a
        valid-looking zero.
        """
        NODATA = -9999.0

        @udf(returnType=RasterType())
        def plus_five(raster):
            return raster.with_bands(raster.as_numpy().astype(np.float64) + 5.0)

        # before band 1 is 0..11 with 3 flagged NODATA; after is 5..16 with 8 flagged,
        # which is the same pixel (index 3) shifted by the +5.
        before = self.spark.range(1).selectExpr(
            "0 AS x", "0 AS y", f"RS_SetBandNoDataValue({FOUR_BAND}, 1, 3.0) AS rast"
        )
        after = before.withColumn("rast", plus_five(col("rast"))).selectExpr(
            "x", "y", "RS_SetBandNoDataValue(rast, 1, 8.0) AS rast"
        )

        @udf(returnType=RasterType())
        def delta(a, b):
            diff = a.as_numpy_masked()[0] - b.as_numpy_masked()[0]
            return a.with_bands(np.where(np.isnan(diff), NODATA, diff), nodata=NODATA)

        result = (
            after.alias("a")
            .join(before.alias("b"), ["x", "y"])
            .select(delta(col("a.rast"), col("b.rast")).alias("d"))
            .selectExpr(
                "RS_BandAsArray(d, 1) AS band",
                "RS_BandNoDataValue(d, 1) AS nodata",
                "RS_Count(d, 1, true) AS counted",
            )
            .first()
        )
        band = list(result["band"])
        assert result["nodata"] == NODATA
        # Pixel 3 was NODATA on both sides: it must be the sentinel, not 0 or 5.
        assert band[3] == pytest.approx(NODATA)
        for index, value in enumerate(band):
            if index != 3:
                assert value == pytest.approx(5.0), f"index {index}"
        # And the sentinel is genuinely excluded from statistics.
        assert result["counted"] == 11

    # ---- "Using rasterio inside a UDF" ------------------------------------

    @requires_spark_34
    def test_rasterio_inside_udf(self):
        """fillnodata must actually fill, and the output must not inherit the hole marker."""
        # Band 1 is 0..11. Flag 5 as NODATA, so index 5 is the hole. Any interpolated
        # replacement is bounded by its neighbours 4 and 6, so it cannot come back as 5
        # unless nothing happened — which is what makes the assertions below meaningful.
        df = self.spark.range(1).withColumn(
            "rast", expr(f"RS_SetBandNoDataValue({FOUR_BAND}, 1, 5.0)")
        )

        @udf(returnType=RasterType())
        def fill_udf(raster):
            valid = ~np.isnan(raster.as_numpy_masked()[0])
            with raster.as_rasterio() as src:
                filled = rasterio.fill.fillnodata(
                    src.read(1), mask=valid.astype(np.uint8)
                )
            return raster.with_bands(filled, nodata=float("nan"))

        result = (
            df.select(fill_udf(col("rast")).alias("filled"))
            .selectExpr(
                "RS_NumBands(filled) AS num_bands",
                "RS_BandAsArray(filled, 1) AS band",
                "RS_BandNoDataValue(filled, 1) AS nodata",
                "RS_Count(filled, 1, true) AS counted",
                "RS_SRID(filled) AS srid",
                "RS_ScaleX(filled) AS scale_x",
            )
            .first()
        )
        assert result["num_bands"] == 1
        assert result["srid"] == 3857
        assert result["scale_x"] == pytest.approx(10.0)

        band = list(result["band"])
        for index, value in enumerate(band):
            if index != 5:
                assert value == pytest.approx(float(index)), f"index {index}"
        # The hole was filled with something derived from its neighbours, and crucially
        # is no longer the original 5.0 that a no-op would have left behind.
        assert band[5] != pytest.approx(5.0)
        assert 4.0 <= band[5] <= 8.0
        # Filling is complete, so the output declares no NODATA and nothing is skipped.
        assert result["nodata"] is None
        assert result["counted"] == 12

    @requires_spark_34
    def test_as_rasterio_does_not_carry_nodata(self):
        """Pins the documented gap: GDAL cannot see Sedona's NODATA."""
        df = self.spark.range(1).withColumn(
            "rast", expr(f"RS_SetBandNoDataValue({FOUR_BAND}, 1, 5.0)")
        )

        @udf(returnType="string")
        def probe(raster):
            with raster.as_rasterio() as src:
                masks = src.read_masks(1)
                return "{}|{}|{}".format(
                    src.nodata,
                    sorted(set(masks.flatten().tolist())),
                    raster.bands_meta[0].nodata,
                )

        gdal_nodata, mask_values, sedona_nodata = (
            df.select(probe(col("rast")).alias("v")).first()["v"].split("|")
        )
        assert gdal_nodata == "None"
        assert mask_values == "[255]"  # everything reported valid
        assert sedona_nodata == "5.0"  # the raster itself knows

    @requires_spark_34
    def test_numpy_scalar_nodata_accepted(self):
        """nodata= accepts NumPy scalars, not just Python floats."""
        raster = self.spark.sql(f"SELECT {FOUR_BAND} AS rast").first()["rast"]
        arr = raster.as_numpy()[0].astype(np.float64)
        for value in (np.float32(-9999), np.float64(-9999), np.int32(-9999)):
            out = raster.with_bands(arr, nodata=value)
            assert out.bands_meta[0].nodata == pytest.approx(-9999.0), type(
                value
            ).__name__

    @requires_spark_34
    def test_nodata_nan_clears_inherited_value(self):
        """float('nan') must clear a NODATA the source has, not silently inherit it."""
        df = self.spark.range(1).withColumn(
            "rast", expr(f"RS_SetBandNoDataValue({FOUR_BAND}, 1, 0.0)")
        )

        @udf(returnType=RasterType())
        def clear_nodata(raster):
            arr = raster.as_numpy()[0].astype(np.float64)
            return raster.with_bands(arr, nodata=float("nan"))

        result = (
            df.select(clear_nodata(col("rast")).alias("out"))
            .selectExpr(
                "RS_BandNoDataValue(out, 1) AS nodata",
                "RS_Count(out, 1, true) AS counted",
            )
            .first()
        )
        assert result["nodata"] is None
        assert result["counted"] == 12  # no pixel is skipped

    @requires_spark_34
    def test_nodata_wider_than_source_dtype(self):
        """A byte source widened to float64 may carry a nodata a byte could not hold."""
        df = self.spark.range(1).withColumn(
            "rast",
            expr(
                "RS_MakeRasterForTesting(1, 'B', 'BandedSampleModel', "
                "4, 3, 100, 100, 10, -10, 0, 0, 3857)"
            ),
        )

        @udf(returnType=RasterType())
        def widen(raster):
            arr = raster.as_numpy()[0].astype(np.float64)
            return raster.with_bands(arr, nodata=-99999.0)

        result = (
            df.select(widen(col("rast")).alias("out"))
            .selectExpr("RS_BandNoDataValue(out, 1) AS nodata")
            .first()
        )
        assert result["nodata"] == -99999.0

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

    def test_nodata_rejects_non_real_scalars(self):
        """Complex values must not silently lose their imaginary part."""
        raster = self.spark.sql(f"SELECT {FOUR_BAND} AS rast").first()["rast"]
        arr = raster.as_numpy()[0].astype(np.float64)
        for value in (np.complex64(1 + 2j), complex(1, 2), True, "5"):
            with pytest.raises(ValueError, match="real number"):
                raster.with_bands(arr, nodata=value)

    def test_nodata_accepts_zero_dimensional_arrays(self):
        """A 0-d array is a scalar and must not be treated as a sequence."""
        raster = self.spark.sql(f"SELECT {FOUR_BAND} AS rast").first()["rast"]
        arr = raster.as_numpy()[0].astype(np.float64)
        out = raster.with_bands(arr, nodata=np.array(-42.0))
        assert out.bands_meta[0].nodata == pytest.approx(-42.0)

    def test_nodata_must_fit_an_integral_output_dtype(self):
        """A fractional NODATA on an integral band is rejected up front.

        The JVM encodes NODATA as a Number of the band's type, so 5.5 on a byte band
        produces a sample dimension that fails on read. Catching it here keeps the
        error close to the mistake.
        """
        raster = self.spark.sql(f"SELECT {FOUR_BAND} AS rast").first()["rast"]
        integral = raster.as_numpy()[0].astype(np.uint8)
        with pytest.raises(ValueError, match="not representable"):
            raster.with_bands(integral, nodata=5.5)
        with pytest.raises(ValueError, match="outside the range"):
            raster.with_bands(integral, nodata=-9999.0)
        # A whole number inside the range is fine.
        out = raster.with_bands(integral, nodata=254.0)
        assert out.bands_meta[0].nodata == pytest.approx(254.0)
