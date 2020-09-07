import os
from os import path

from tests.tools import tests_path

data_path = path.abspath(path.dirname(__file__))


def create_data_path(relative_path: str) -> str:
    return os.path.join(data_path, relative_path)


mixed_wkb_geometry_input_location = create_data_path("county_small_wkb.tsv")
mixed_wkt_geometry_input_location = os.path.join(tests_path, "data/county_small.tsv")
mixed_wkt_geometry_input_location_1 = os.path.join(tests_path, "data/county_small_1.tsv")
shape_file_input_location = create_data_path("shapefiles/dbf")
shape_file_with_missing_trailing_input_location = create_data_path("shapefiles/missing")
geojson_input_location = create_data_path("testPolygon.json")
area_lm_point_input_location = create_data_path("arealm.csv")
csv_point_input_location = create_data_path("testpoint.csv")
csv_polygon_input_location = create_data_path("testenvelope.csv")
csv_polygon1_input_location = create_data_path("equalitycheckfiles/testequals_envelope1.csv")
csv_polygon2_input_location = create_data_path("equalitycheckfiles/testequals_envelope2.csv")
csv_polygon1_random_input_location = create_data_path("equalitycheckfiles/testequals_envelope1_random.csv")
csv_polygon2_random_input_location = create_data_path("equalitycheckfiles/testequals_envelope2_random.csv")
overlap_polygon_input_location = create_data_path("testenvelope_overlap.csv")
union_polygon_input_location = create_data_path("testunion.csv")
csv_point1_input_location = create_data_path("equalitycheckfiles/testequals_point1.csv")
csv_point2_input_location = create_data_path("equalitycheckfiles/testequals_point2.csv")
geojson_id_input_location = create_data_path("testContainsId.json")
