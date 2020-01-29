import os

from geo_pyspark.core.enums import FileDataSplitter
from geo_pyspark.core.geom.envelope import Envelope
from tests.tools import tests_path

input_location = os.path.join(tests_path, "resources/primaryroads-linestring.csv")
query_window_set = os.path.join(tests_path, "resources/zcta510-small.csv")
offset = 0
splitter = FileDataSplitter.CSV
grid_type = "rtree"
index_type = "rtree"
num_partitions = 5
distance = 0.01
query_polygon_set = os.path.join(tests_path, "resources/primaryroads-polygon.csv")
input_count = 3000
input_boundary = Envelope(minx=-123.393766, maxx=-65.648659, miny=17.982169, maxy=49.002374)
input_boundary_2 = Envelope(minx=-123.393766, maxx=-65.649956, miny=17.982169, maxy=49.002374)
match_count = 535
match_with_origin_with_duplicates_count = 875

transformed_envelope = Envelope(14313844.29433424, 16791709.85358734, 942450.5989896542, 8474779.278028419)
transformed_envelope_2 = Envelope(14313844.29433424, 16791709.85358734, 942450.5989896542, 8474645.488977494)
