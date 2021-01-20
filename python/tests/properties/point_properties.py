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

from sedona.core.enums import FileDataSplitter
from sedona.core.geom.envelope import Envelope
from tests.tools import tests_resource

input_location = os.path.join(tests_resource, "arealm-small.csv")
query_window_set = os.path.join(tests_resource, "zcta510-small.csv")
offset = 1
splitter = FileDataSplitter.CSV
grid_type = "rtree"
index_type = "rtree"
num_partitions = 11
distance = 0.01
query_polygon_set = "primaryroads-polygon.csv"
input_count = 3000
input_boundary = Envelope(
    minx=-173.120769,
    maxx=-84.965961,
    miny=30.244859,
    maxy=71.355134
)
rectangle_match_count = 103
rectangle_with_original_duplicates_count = 103
polygon_match_count = 472
polygon_match_with_original_duplicates_count = 562

transformed_envelope = Envelope(14313844.294334238, 16587207.463797076, 942450.5989896103, 6697987.652517772)
crs_point_test = os.path.join(tests_resource, "crs-test-point.csv")
crs_envelope = Envelope(26.992172, 71.35513400000001, -179.147236, 179.475569)
crs_envelope_transformed = Envelope(-5446655.086752236, 1983668.3828524568, 534241.8622328975, 6143259.025545624)