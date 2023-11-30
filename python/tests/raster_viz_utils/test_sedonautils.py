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

from IPython.display import display, HTML
from tests.test_base import TestBase
from sedona.raster_utils.SedonaUtils import SedonaUtils
from tests import world_map_raster_input_location


class TestSedonaUtils(TestBase):
    def test_display_image(self):
        raster_bin_df = self.spark.read.format('binaryFile').load(world_map_raster_input_location)
        raster_bin_df.createOrReplaceTempView('raster_binary_table')
        raster_df = self.spark.sql('SELECT RS_FromGeotiff(content) as raster from raster_binary_table')
        raster_image_df = raster_df.selectExpr('RS_AsImage(raster) as rast_img')
        html_call = SedonaUtils.display_image(raster_image_df)
        assert html_call is None  # just test that this function was called and returned no output
