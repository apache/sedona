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

from tests.tools import tests_resource


mixed_wkb_geometry_input_location = os.path.join(tests_resource, "county_small_wkb.tsv")
mixed_wkt_geometry_input_location = os.path.join(tests_resource, "county_small.tsv")
shape_file_input_location = os.path.join(tests_resource, "shapefiles/dbf")
shape_file_with_missing_trailing_input_location = os.path.join(tests_resource, "shapefiles/missing")
geojson_input_location = os.path.join(tests_resource, "testPolygon.json")
area_lm_point_input_location = os.path.join(tests_resource, "arealm.csv")
csv_point_input_location = os.path.join(tests_resource, "testpoint.csv")
csv_polygon_input_location = os.path.join(tests_resource, "testenvelope.csv")
csv_polygon1_input_location = os.path.join(tests_resource, "equalitycheckfiles/testequals_envelope1.csv")
csv_polygon2_input_location = os.path.join(tests_resource, "equalitycheckfiles/testequals_envelope2.csv")
csv_polygon1_random_input_location = os.path.join(tests_resource, "equalitycheckfiles/testequals_envelope1_random.csv")
csv_polygon2_random_input_location = os.path.join(tests_resource, "equalitycheckfiles/testequals_envelope2_random.csv")
overlap_polygon_input_location = os.path.join(tests_resource, "testenvelope_overlap.csv")
union_polygon_input_location = os.path.join(tests_resource, "testunion.csv")
csv_point1_input_location = os.path.join(tests_resource, "equalitycheckfiles/testequals_point1.csv")
csv_point2_input_location = os.path.join(tests_resource, "equalitycheckfiles/testequals_point2.csv")
geojson_id_input_location = os.path.join(tests_resource, "testContainsId.json")
geoparquet_input_location = os.path.join(tests_resource, "geoparquet/example1.parquet")
plain_parquet_input_location = os.path.join(tests_resource, "geoparquet/plain.parquet")
