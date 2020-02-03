import os

from geo_pyspark.core.enums import FileDataSplitter
from geo_pyspark.core.geom.envelope import Envelope
from tests.tools import tests_path

input_location = os.path.join(tests_path, "resources/arealm-small.csv")
query_window_set = os.path.join("zcta510-small.csv")
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

transformed_envelope = Envelope(14313844.29433424, 16587207.463797055, 942450.5989896542, 6697987.652517834)
crs_point_test = os.path.join(tests_path, "resources/crs-test-point.csv")
crs_envelope = Envelope(26.992172, 71.35513400000001, -179.147236, 179.475569)
crs_envelope_transformed = Envelope(-5446655.086752228, 1983668.382852457, 534241.8622328962, 6143259.02554563)