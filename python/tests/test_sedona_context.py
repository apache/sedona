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


from tests.test_base import TestBase
from sedona.core.SpatialRDD import PolygonRDD
from sedona.core.enums import FileDataSplitter
from sedona.utils.adapter import Adapter
from tests import geojson_input_location
from sedona.spark import *

class TestSedonaContext(TestBase):

    def test_getSedonaContext(self):
        sparkCurrent = SedonaContext.getSedonaContext()
        spatial_rdd = PolygonRDD(
            sparkCurrent.sparkContext, geojson_input_location, FileDataSplitter.GEOJSON, True
        )

        spatial_rdd.analyze()
        df = Adapter.toDf(spatial_rdd, sparkCurrent)

        assert (df.columns[1] == "STATEFP")
