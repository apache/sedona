import os

from geo_pyspark.core.enums import FileDataSplitter, IndexType
from geo_pyspark.core.geom.envelope import Envelope
from tests.tools import tests_path

input_location = os.path.join(tests_path, "resources/primaryroads-polygon.csv")
query_window_set = os.path.join(tests_path, "resources/zcta510-small.csv")
offset = 0
splitter = FileDataSplitter.CSV
grid_type = "rtree"
index_type = "rtree"
num_partitions = 5
distance = 0.01
input_location_query_polygon = os.path.join(tests_path, "resources/crs-test-polygon.csv")
query_polygon_count = 13361
query_envelope = Envelope(14313844.29433424, 16802290.85383074, 942450.5989896542, 8631908.270651951)
query_polygon_set = os.path.join(tests_path, "resources/primaryroads-polygon.csv")
input_location_geo_json = os.path.join(tests_path, "resources/testPolygon.json")
input_location_wkt = os.path.join(tests_path, "resources/county_small.tsv")
input_location_wkb = os.path.join(tests_path, "resources/county_small_wkb.tsv")
input_count = 3000
input_boundary = Envelope(minx=-158.104182, maxx=-66.03575, miny=17.986328, maxy=48.645133)
contains_match_count = 6941
contains_match_with_original_duplicates_count = 9334
intersects_match_count = 24323
intersects_match_with_original_duplicates_count = 32726

polygon_rdd_input_location = os.path.join(tests_path, "resources/primaryroads-polygon.csv")
polygon_rdd_splitter = FileDataSplitter.CSV
polygon_rdd_index_type = IndexType.RTREE
polygon_rdd_num_partitions = 5
polygon_rdd_start_offset = 0
polygon_rdd_end_offset = 9