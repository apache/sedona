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

from sedona.core.enums import FileDataSplitter, IndexType
from sedona.core.geom.envelope import Envelope
from tests.tools import tests_resource

input_location = os.path.join(tests_resource, "primaryroads-polygon.csv")
query_window_set = os.path.join(tests_resource, "zcta510-small.csv")
offset = 0
splitter = FileDataSplitter.CSV
grid_type = "kdbtree"
index_type = "rtree"
num_partitions = 5
distance = 0.01
input_location_query_polygon = os.path.join(tests_resource, "crs-test-polygon.csv")
query_polygon_count = 13361

query_envelope = Envelope(14313844.294334238, 16802290.853830762, 942450.5989896103, 8631908.270651892)
query_polygon_set = os.path.join(tests_resource, "primaryroads-polygon.csv")
input_location_geo_json = os.path.join(tests_resource, "testPolygon.json")
input_location_wkt = os.path.join(tests_resource, "county_small.tsv")
input_location_wkb = os.path.join(tests_resource, "county_small_wkb.tsv")
input_count = 3000
input_boundary = Envelope(minx=-158.104182, maxx=-66.03575, miny=17.986328, maxy=48.645133)
contains_match_count = 6941
contains_match_with_original_duplicates_count = 9334
intersects_match_count = 24323
intersects_match_with_original_duplicates_count = 32726

polygon_rdd_input_location = os.path.join(tests_resource, "primaryroads-polygon.csv")
polygon_rdd_splitter = FileDataSplitter.CSV
polygon_rdd_index_type = IndexType.RTREE
polygon_rdd_num_partitions = 5
polygon_rdd_start_offset = 0
polygon_rdd_end_offset = 9
