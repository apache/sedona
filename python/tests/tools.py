from os import path

from shapely.geometry import Point

from geo_pyspark.utils.spatial_rdd_parser import GeoData

tests_path = path.abspath(path.dirname(__file__))


def distance_sorting_functions(geo_data: GeoData, query_point: Point):
    return geo_data.geom.distance(query_point)
