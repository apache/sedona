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

import os

from sedona.core.formatMapper.wkb_reader import WkbReader
from tests.test_base import TestBase
from tests.tools import tests_resource


class TestWkbReader(TestBase):

    def test_read_to_geometry_rdd(self):
        wkb_geometries = os.path.join(tests_resource, "county_small_wkb.tsv")

        wkb_rdd = WkbReader.readToGeometryRDD(self.sc, wkb_geometries, 0, True, False)
        assert wkb_rdd.rawSpatialRDD.count() == 103
