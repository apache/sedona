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

input_location = os.path.join(tests_resource, "primaryroads-linestring.csv")
query_window_set = os.path.join(tests_resource, "zcta510-small.csv")
offset = 0
splitter = FileDataSplitter.CSV
grid_type = "kdbtree"
index_type = "rtree"
num_partitions = 5
distance = 0.01
query_polygon_set = os.path.join(tests_resource, "primaryroads-polygon.csv")
input_count = 3000
input_boundary = Envelope(minx=-123.393766, maxx=-65.648659, miny=17.982169, maxy=49.002374)
input_boundary_2 = Envelope(minx=-123.393766, maxx=-65.649956, miny=17.982169, maxy=49.002374)
match_count = 535
match_with_origin_with_duplicates_count = 875

transformed_envelope = Envelope(14313844.294334238, 16791709.853587367, 942450.5989896103, 8474779.278028358)
transformed_envelope_2 = Envelope(14313844.294334238, 16791709.853587367, 942450.5989896103, 8474645.488977432)
