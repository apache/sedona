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

import pytest
from tests.test_base import SPARK_REMOTE, TestBase


class TestRaster(TestBase):

    def test_raster_df_repr(self):
        """DataFrame containing raster columns should be represented without
        exception.

        """
        raster_df = self.spark.sql("SELECT RS_MakeEmptyRaster(1, 10, 10, 0, 0, 1) rast")
        assert "rast: udt" in repr(raster_df)

    @pytest.mark.skipif(SPARK_REMOTE is not None, reason="needs the JVM gateway")
    def test_jaiext_operations_registered(self):
        """Every JAI-EXT module ships its own META-INF/registryFile.jaiext. A jar that
        bundles them must merge those files, otherwise all but one operation silently
        fall back to core JAI.

        """
        jaiext = self.spark.sparkContext._jvm.it.geosolutions.jaiext.JAIExt
        jaiext.initJAIEXT(True)
        for op in [
            "Affine",
            "BandMerge",
            "Crop",
            "Mosaic",
            "Scale",
            "Translate",
            "Warp",
        ]:
            assert jaiext.isJAIExtOperation(op), op
