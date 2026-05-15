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
