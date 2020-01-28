import os

from shapely.geometry import Point

from geo_pyspark.core.enums import FileDataSplitter, GridType, IndexType
from geo_pyspark.core.geom_types import Envelope
from tests.tools import tests_path

input_location = os.path.join(tests_path, "resources/crs-test-point.csv")
offset = 0
splitter = FileDataSplitter.CSV
grid_type = GridType.RTREE
index_type = IndexType.RTREE
num_partitions = 11
distance = 0.01
input_location_query_polygon = os.path.join(tests_path, "resources/crs-test-polygon.csv")
loop_times = 5
query_envelope = Envelope(30.01, 40.01, -90.01, -80.01)
query_point = Point(34.01, -84.01)
top_k = 100