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

import math

import numpy as np
import pyspark
import pytest
from pyspark.sql.functions import col, expr, udf
from tests.test_base import TestBase

from sedona.spark.sql.types import RasterType


class TestFlexibleBands(TestBase):
    @pytest.mark.skipif(
        pyspark.__version__ < "3.4", reason="requires Spark 3.4 or higher"
    )
    def test_reduce_bands_4_to_1(self):
        """Reduce from 4 bands to 1 band (NDVI-like calculation)."""
        spark = self.spark

        @udf(returnType=RasterType())
        def ndvi_like(raster):
            arr = raster.as_numpy().astype(np.float32)
            nir = arr[3]
            red = arr[2]
            ndvi = (nir - red) / (nir + red + 1e-10)
            return raster.with_bands(ndvi)  # (H, W) → 1 band

        df = spark.range(1).withColumn(
            "rast",
            expr(
                "RS_MakeRasterForTesting(4, 'F', 'BandedSampleModel', "
                "4, 3, 100, 100, 10, -10, 0, 0, 3857)"
            ),
        )
        result = (
            df.select(ndvi_like(col("rast")).alias("rast2"))
            .selectExpr(
                "RS_NumBands(rast2) as num_bands",
                "RS_BandAsArray(rast2, 1) as band",
            )
            .first()
        )
        assert result["num_bands"] == 1
        band = result["band"]
        assert len(band) == 4 * 3
        for val in band:
            assert math.isfinite(val), f"Expected finite value, got {val}"

    @pytest.mark.skipif(
        pyspark.__version__ < "3.4", reason="requires Spark 3.4 or higher"
    )
    def test_increase_bands_3_to_6(self):
        """Increase from 3 bands to 6 (original + squared features)."""
        spark = self.spark

        @udf(returnType=RasterType())
        def stack_features(raster):
            arr = raster.as_numpy().astype(np.float64)
            derived = arr**2
            stacked = np.concatenate([arr, derived], axis=0)
            return raster.with_bands(stacked)

        df = spark.range(1).withColumn(
            "rast",
            expr(
                "RS_MakeRasterForTesting(3, 'D', 'BandedSampleModel', "
                "4, 3, 100, 100, 10, -10, 0, 0, 3857)"
            ),
        )
        result = (
            df.select(stack_features(col("rast")).alias("rast2"))
            .selectExpr(
                "RS_NumBands(rast2) as num_bands",
                "RS_BandAsArray(rast2, 1) as band1",
                "RS_BandAsArray(rast2, 4) as band4",
            )
            .first()
        )
        assert result["num_bands"] == 6
        # Band 1: original band 0 = 0 + y*4 + x
        band1 = result["band1"]
        for y in range(3):
            for x in range(4):
                expected = float(0 + y * 4 + x)
                assert band1[y * 4 + x] == expected
        # Band 4: derived = band 0 squared
        band4 = result["band4"]
        for y in range(3):
            for x in range(4):
                original = float(0 + y * 4 + x)
                assert band4[y * 4 + x] == original**2

    @pytest.mark.skipif(
        pyspark.__version__ < "3.4", reason="requires Spark 3.4 or higher"
    )
    def test_dtype_change_int_to_float(self):
        """Change dtype from int32 to float32 while keeping same band count."""
        spark = self.spark

        @udf(returnType=RasterType())
        def normalize(raster):
            arr = raster.as_numpy().astype(np.float32)
            normalized = arr / (arr.max() + 1e-10)
            return raster.with_bands(normalized)

        df = spark.range(1).withColumn(
            "rast",
            expr(
                "RS_MakeRasterForTesting(1, 'I', 'BandedSampleModel', "
                "4, 3, 100, 100, 10, -10, 0, 0, 3857)"
            ),
        )
        result = (
            df.select(normalize(col("rast")).alias("rast2"))
            .selectExpr("RS_BandAsArray(rast2, 1) as band")
            .first()
        )
        band = result["band"]
        for val in band:
            assert 0.0 <= val <= 1.0, f"Expected [0,1], got {val}"

    @pytest.mark.skipif(
        pyspark.__version__ < "3.4", reason="requires Spark 3.4 or higher"
    )
    def test_mixed_band_and_dtype_change(self):
        """Simultaneous band count reduction and dtype change."""
        spark = self.spark

        @udf(returnType=RasterType())
        def mean_bands(raster):
            arr = raster.as_numpy().astype(np.float32)
            mean = np.mean(arr, axis=0, keepdims=True)
            return raster.with_bands(mean)

        df = spark.range(1).withColumn(
            "rast",
            expr(
                "RS_MakeRasterForTesting(3, 'I', 'BandedSampleModel', "
                "4, 3, 100, 100, 10, -10, 0, 0, 3857)"
            ),
        )
        result = (
            df.select(mean_bands(col("rast")).alias("rast2"))
            .selectExpr(
                "RS_NumBands(rast2) as num_bands",
                "RS_BandAsArray(rast2, 1) as band",
            )
            .first()
        )
        assert result["num_bands"] == 1
        band = result["band"]
        # Mean of 3 bands: mean(b + y*4+x for b in 0,1,2) = 1 + y*4 + x
        for y in range(3):
            for x in range(4):
                expected = 1.0 + y * 4 + x
                assert abs(band[y * 4 + x] - expected) < 0.01

    @pytest.mark.skipif(
        pyspark.__version__ < "3.4", reason="requires Spark 3.4 or higher"
    )
    def test_jvm_mapalgebra_after_band_change(self):
        """JVM-side RS_MapAlgebra works on a raster that had bands changed by UDF."""
        spark = self.spark

        @udf(returnType=RasterType())
        def select_band1(raster):
            arr = raster.as_numpy()
            return raster.with_bands(arr[0:1].astype(np.float64))

        df = spark.range(1).withColumn(
            "rast",
            expr(
                "RS_MakeRasterForTesting(3, 'D', 'BandedSampleModel', "
                "4, 3, 100, 100, 10, -10, 0, 0, 3857)"
            ),
        )
        result = (
            df.select(select_band1(col("rast")).alias("rast2"))
            .selectExpr("RS_MapAlgebra(rast2, 'D', 'out[0] = rast[0] + 100;') as rast3")
            .selectExpr("RS_BandAsArray(rast3, 1) as band")
            .first()
        )
        band = result["band"]
        for y in range(3):
            for x in range(4):
                expected = float(y * 4 + x + 100)
                assert band[y * 4 + x] == expected

    @pytest.mark.skipif(
        pyspark.__version__ < "3.4", reason="requires Spark 3.4 or higher"
    )
    def test_reduce_bands_8_to_1_argmax(self):
        """8 bands to 1 band via argmax (KMeans-like cluster assignment)."""
        spark = self.spark

        @udf(returnType=RasterType())
        def argmax_band(raster):
            arr = raster.as_numpy().astype(np.float32)
            result = np.argmax(arr, axis=0).astype(np.float32)
            return raster.with_bands(result)

        df = spark.range(1).withColumn(
            "rast",
            expr(
                "RS_MakeRasterForTesting(8, 'F', 'BandedSampleModel', "
                "4, 3, 100, 100, 10, -10, 0, 0, 3857)"
            ),
        )
        result = (
            df.select(argmax_band(col("rast")).alias("rast2"))
            .selectExpr(
                "RS_NumBands(rast2) as num_bands",
                "RS_BandAsArray(rast2, 1) as band",
            )
            .first()
        )
        assert result["num_bands"] == 1
        band = result["band"]
        # Band 7 has highest values (7 + y*4 + x), so argmax = 7.0
        for val in band:
            assert val == 7.0, f"Expected 7.0, got {val}"

    @pytest.mark.skipif(
        pyspark.__version__ < "3.4", reason="requires Spark 3.4 or higher"
    )
    def test_metadata_survives_band_change(self):
        """CRS, affine transform, and dimensions survive a band count change."""
        spark = self.spark

        @udf(returnType=RasterType())
        def reduce_to_1(raster):
            arr = raster.as_numpy().astype(np.float64)
            return raster.with_bands(arr[0:1])

        df = spark.range(1).withColumn(
            "rast",
            expr(
                "RS_MakeRasterForTesting(3, 'D', 'BandedSampleModel', "
                "4, 3, 100, 100, 10, -10, 0, 0, 3857)"
            ),
        )
        result = (
            df.select(reduce_to_1(col("rast")).alias("rast2"))
            .selectExpr(
                "RS_NumBands(rast2) as num_bands",
                "RS_Width(rast2) as width",
                "RS_Height(rast2) as height",
                "RS_ScaleX(rast2) as scale_x",
                "RS_ScaleY(rast2) as scale_y",
                "RS_SRID(rast2) as srid",
            )
            .first()
        )
        assert result["num_bands"] == 1
        assert result["width"] == 4
        assert result["height"] == 3
        assert abs(result["scale_x"] - 10.0) < 0.001
        assert abs(result["scale_y"] - (-10.0)) < 0.001
        assert result["srid"] == 3857

    @pytest.mark.skipif(
        pyspark.__version__ < "3.4", reason="requires Spark 3.4 or higher"
    )
    def test_nodata_defaults_to_inherited(self):
        """Without nodata=, each output band inherits it from the input band."""
        spark = self.spark

        @udf(returnType=RasterType())
        def reduce_to_1(raster):
            return raster.with_bands(raster.as_numpy()[0].astype(np.float64))

        df = spark.range(1).withColumn(
            "rast",
            expr(
                "RS_SetBandNoDataValue(RS_MakeRasterForTesting("
                "4, 'D', 'BandedSampleModel', 4, 3, 100, 100, 10, -10, 0, 0, 3857), 1, 0.0)"
            ),
        )
        result = (
            df.select(reduce_to_1(col("rast")).alias("rast2"))
            .selectExpr("RS_BandNoDataValue(rast2, 1) as nodata")
            .first()
        )
        assert result["nodata"] == 0.0

    @pytest.mark.skipif(
        pyspark.__version__ < "3.4", reason="requires Spark 3.4 or higher"
    )
    def test_nodata_scalar_overrides_inherited(self):
        """A scalar nodata= reaches the JVM instead of the inherited value."""
        spark = self.spark

        @udf(returnType=RasterType())
        def mask(raster):
            band1 = raster.as_numpy()[0]
            return raster.with_bands((band1 < 6).astype(np.float64), nodata=-9999.0)

        df = spark.range(1).withColumn(
            "rast",
            expr(
                "RS_SetBandNoDataValue(RS_MakeRasterForTesting("
                "4, 'D', 'BandedSampleModel', 4, 3, 100, 100, 10, -10, 0, 0, 3857), 1, 0.0)"
            ),
        )
        result = (
            df.select(mask(col("rast")).alias("rast2"))
            .selectExpr(
                "RS_BandNoDataValue(rast2, 1) as nodata",
                "RS_Count(rast2, 1, true) as counted",
                "RS_Count(rast2, 1, false) as total",
            )
            .first()
        )
        # -9999 replaces the inherited 0, so the mask's zeros are real data again.
        assert result["nodata"] == -9999.0
        assert result["counted"] == 12
        assert result["total"] == 12

    @pytest.mark.skipif(
        pyspark.__version__ < "3.4", reason="requires Spark 3.4 or higher"
    )
    def test_nodata_per_band_sequence(self):
        """A sequence nodata= assigns a different value to each output band."""
        spark = self.spark

        @udf(returnType=RasterType())
        def widen(raster):
            arr = raster.as_numpy().astype(np.float64)
            return raster.with_bands(
                np.concatenate([arr, arr], axis=0), nodata=[-1.0, -2.0, -3.0, -4.0] * 2
            )

        df = spark.range(1).withColumn(
            "rast",
            expr(
                "RS_MakeRasterForTesting(4, 'D', 'BandedSampleModel', "
                "4, 3, 100, 100, 10, -10, 0, 0, 3857)"
            ),
        )
        result = (
            df.select(widen(col("rast")).alias("rast2"))
            .selectExpr(
                "RS_NumBands(rast2) as num_bands",
                *[f"RS_BandNoDataValue(rast2, {b}) as nodata{b}" for b in range(1, 9)],
            )
            .first()
        )
        assert result["num_bands"] == 8
        for band, expected in enumerate([-1.0, -2.0, -3.0, -4.0] * 2, start=1):
            assert result[f"nodata{band}"] == expected, f"band {band}"

    @pytest.mark.skipif(
        pyspark.__version__ < "3.4", reason="requires Spark 3.4 or higher"
    )
    def test_nodata_agrees_between_python_and_jvm_when_widening(self):
        """bands_meta and RS_BandNoDataValue report the same value for added bands."""
        spark = self.spark

        @udf(returnType="string")
        def python_view(raster):
            arr = raster.as_numpy().astype(np.float64)
            out = raster.with_bands(np.concatenate([arr, arr], axis=0))
            return ",".join(str(bm.nodata) for bm in out.bands_meta)

        @udf(returnType=RasterType())
        def widen(raster):
            arr = raster.as_numpy().astype(np.float64)
            return raster.with_bands(np.concatenate([arr, arr], axis=0))

        df = spark.range(1).withColumn(
            "rast",
            expr(
                "RS_SetBandNoDataValue(RS_MakeRasterForTesting("
                "4, 'D', 'BandedSampleModel', 4, 3, 100, 100, 10, -10, 0, 0, 3857), 4, -1.0)"
            ),
        )
        python_nodata = df.select(python_view(col("rast")).alias("v")).first()["v"]
        jvm = (
            df.select(widen(col("rast")).alias("rast2"))
            .selectExpr(
                *[f"RS_BandNoDataValue(rast2, {b}) as nodata{b}" for b in range(1, 9)]
            )
            .first()
        )
        # Bands 4-8 all replay band 4's category blob, which carries -1.0.
        assert python_nodata.split(",")[3:] == ["-1.0"] * 5
        for band in range(4, 9):
            assert jvm[f"nodata{band}"] == -1.0, f"band {band}"

    def test_nodata_sequence_length_is_validated(self):
        """A nodata= sequence of the wrong length is rejected."""
        raster = self.spark.sql(
            "SELECT RS_MakeRasterForTesting(4, 'D', 'BandedSampleModel', "
            "4, 3, 100, 100, 10, -10, 0, 0, 3857) AS rast"
        ).first()["rast"]
        with pytest.raises(ValueError, match="nodata has 2 entries"):
            raster.with_bands(np.zeros((3, 4), dtype=np.float64), nodata=[1.0, 2.0])
