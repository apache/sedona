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

from tests import world_map_raster_input_location
from tests.test_base import TestBase

from sedona.spark import SedonaUtils


class TestSedonaUtils(TestBase):
    def test_display_image(self):
        raster_bin_df = self.spark.read.format("binaryFile").load(
            world_map_raster_input_location
        )
        raster_df = raster_bin_df.selectExpr("RS_FromGeotiff(content) as raster")
        raster_image_df = raster_df.selectExpr("RS_AsImage(raster) as rast_img")
        html_call = SedonaUtils.display_image(raster_image_df)
        assert (
            html_call is None
        )  # just test that this function was called and returned no output

    def test_display_image_raw_raster(self):
        """Test that display_image auto-detects raster columns and applies RS_AsImage."""
        raster_bin_df = self.spark.read.format("binaryFile").load(
            world_map_raster_input_location
        )
        raster_df = raster_bin_df.selectExpr("RS_FromGeotiff(content) as raster")
        html_call = SedonaUtils.display_image(raster_df)
        assert html_call is None

    def test_display_image_preserves_non_raster_columns(self):
        """Test that non-raster columns are preserved alongside raster columns."""
        raster_bin_df = self.spark.read.format("binaryFile").load(
            world_map_raster_input_location
        )
        raster_df = raster_bin_df.selectExpr(
            "path", "RS_FromGeotiff(content) as raster"
        )
        html_call = SedonaUtils.display_image(raster_df)
        assert html_call is None
