#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import pytest
import rasterio
import numpy as np

from tests.test_base import TestBase
from pyspark.sql.functions import expr
from sedona.sql.types import RasterType

from tests import world_map_raster_input_location

class TestRasterSerde(TestBase):
    def test_empty_raster(self):
        df = TestRasterSerde.spark.sql("SELECT RS_MakeEmptyRaster(2, 100, 200, 1000, 2000, 1) as raster")
        raster = df.first()[0]
        assert raster.width == 100 and raster.height == 200 and len(raster.bands_meta) == 2
        assert raster.affine_trans.ip_x == 1000 and raster.affine_trans.ip_y == 2000
        assert raster.affine_trans.scale_x == 1 and raster.affine_trans.scale_y == -1

    def test_banded_sample_model(self):
        df = TestRasterSerde.spark.sql("SELECT RS_MakeRasterForTesting(3, 'I', 'BandedSampleModel', 10, 8, 100, 100, 10, -10, 0, 0, 3857) as raster")
        raster = df.first()[0]
        assert raster.width == 10 and raster.height == 8 and len(raster.bands_meta) == 3
        self.validate_test_raster(raster)

    def test_pixel_interleaved_sample_model(self):
        df = TestRasterSerde.spark.sql("SELECT RS_MakeRasterForTesting(3, 'I', 'PixelInterleavedSampleModel', 10, 10, 100, 100, 10, -10, 0, 0, 3857) as raster")
        raster = df.first()[0]
        assert raster.width == 10 and raster.height == 10 and len(raster.bands_meta) == 3
        self.validate_test_raster(raster)
        df = TestRasterSerde.spark.sql("SELECT RS_MakeRasterForTesting(4, 'I', 'PixelInterleavedSampleModelComplex', 8, 10, 100, 100, 10, -10, 0, 0, 3857) as raster")
        raster = df.first()[0]
        assert raster.width == 8 and raster.height == 10 and len(raster.bands_meta) == 4
        self.validate_test_raster(raster)

    def test_component_sample_model(self):
        for pixel_type in ['B', 'S', 'US', 'I', 'F', 'D']:
            df = TestRasterSerde.spark.sql("SELECT RS_MakeRasterForTesting(4, '{}', 'ComponentSampleModel', 10, 10, 100, 100, 10, -10, 0, 0, 3857) as raster".format(pixel_type))
            raster = df.first()[0]
            assert raster.width == 10 and raster.height == 10 and len(raster.bands_meta) == 4
            self.validate_test_raster(raster)

    def test_multi_pixel_packed_sample_model(self):
        df = TestRasterSerde.spark.sql("SELECT RS_MakeRasterForTesting(1, 'B', 'MultiPixelPackedSampleModel', 10, 10, 100, 100, 10, -10, 0, 0, 3857) as raster")
        raster = df.first()[0]
        assert raster.width == 10 and raster.height == 10 and len(raster.bands_meta) == 1
        self.validate_test_raster(raster, packed=True)

    def test_single_pixel_packed_sample_model(self):
        df = TestRasterSerde.spark.sql("SELECT RS_MakeRasterForTesting(4, 'I', 'SinglePixelPackedSampleModel', 10, 10, 100, 100, 10, -10, 0, 0, 3857) as raster")
        raster = df.first()[0]
        assert raster.width == 10 and raster.height == 10 and len(raster.bands_meta) == 4
        self.validate_test_raster(raster, packed=True)

    def test_raster_read_from_geotiff(self):
        raster_path = world_map_raster_input_location
        r_orig = rasterio.open(raster_path)
        band = r_orig.read(1)
        band_masked = np.where(band == 0, np.nan, band)
        df = TestRasterSerde.spark.read.format("binaryFile").load(raster_path).selectExpr("RS_FromGeoTiff(content) as raster")
        raster = df.first()[0]
        assert raster.width == r_orig.width
        assert raster.height == r_orig.height
        assert raster.bands_meta[0].nodata == 0

        # test as_rasterio
        assert (band == raster.as_numpy()[0, :, :]).all()
        ds = raster.as_rasterio()
        assert ds.crs is not None
        band_actual = ds.read(1)
        assert (band == band_actual).all()

        # test as_numpy
        arr = raster.as_numpy()
        assert (arr[0, :, :] == band).all()

        # test as_numpy_masked
        arr = raster.as_numpy_masked()[0, :, :]
        assert np.array_equal(arr, band_masked) or np.array_equal(np.isnan(arr), np.isnan(band_masked))

        raster.close()
        r_orig.close()

    def test_to_pandas(self):
        spark = TestRasterSerde.spark
        df = spark.sql("SELECT RS_MakeRasterForTesting(3, 'I', 'BandedSampleModel', 10, 8, 100, 100, 10, -10, 0, 0, 3857) as raster")
        pandas_df = df.toPandas()
        raster = pandas_df.iloc[0]['raster']
        self.validate_test_raster(raster)

    def validate_test_raster(self, raster, packed = False):
        arr = raster.as_numpy()
        ds = raster.as_rasterio()
        bands, height, width = arr.shape
        assert bands > 0 and width > 0 and height > 0
        assert ds.crs is not None
        for b in range(bands):
            band = ds.read(b + 1)
            for y in range(height):
                for x in range(width):
                    expected = b + y * width + x
                    if packed:
                        expected = expected % 16
                    assert arr[b, y, x] == expected
                    assert band[y, x] == expected
