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

from sedona.core.formatMapper import WktReader
from tests.test_base import TestBase
from tests.tools import tests_resource


class TestWktReader(TestBase):

    def test_read_to_geometry_rdd(self):
        wkt_geometries = os.path.join(tests_resource, "county_small.tsv")
        wkt_rdd = WktReader.readToGeometryRDD(self.sc, wkt_geometries, 0, True, False)
        assert wkt_rdd.rawSpatialRDD.count() == 103
        print(wkt_rdd.rawSpatialRDD.collect())
