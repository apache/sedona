import os
from os import path
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructField, StructType
from shapely import wkt

from geospark.sql.types import GeometryType

data_path = path.abspath(path.dirname(__file__))


def create_sample_polygons_df(spark: SparkSession, number_of_polygons: int) -> DataFrame:
    return resource_file_to_dataframe(spark, "sample_polygons").limit(number_of_polygons)


def create_sample_points_df(spark: SparkSession, number_of_points: int) -> DataFrame:
    return resource_file_to_dataframe(spark, "sample_points").limit(number_of_points)


def create_simple_polygons_df(spark: SparkSession, number_of_polygons: int) -> DataFrame:
    return resource_file_to_dataframe(spark, "simple_polygons").limit(number_of_polygons)


def create_sample_lines_df(spark: SparkSession, number_of_lines: int) -> DataFrame:
    return resource_file_to_dataframe(spark, "sample_lines").limit(number_of_lines)


def create_sample_polygons(number_of_polygons: int) -> List:
    return load_from_resources(data_path, "sample_polygons")[: number_of_polygons]


def create_sample_points(number_of_points: int) -> List:
    return load_from_resources(data_path, "sample_points")[: number_of_points]


def create_simple_polygons(number_of_polygons: int) -> List:
    return load_from_resources(data_path, "simple_polygons")[: number_of_polygons]


def create_sample_lines(number_of_lines: int) -> List:
    return load_from_resources(data_path, "sample_lines")[: number_of_lines]


def resource_file_to_dataframe(spark: SparkSession, file_path: str) -> DataFrame:
    geometries = load_from_resources(data_path, file_path)
    schema = StructType([
        StructField("geom", GeometryType(), True)
    ])
    return spark.createDataFrame([[el] for el in geometries], schema=schema)


def load_from_resources(data_path: str, file_path: str) -> List:
    with open(os.path.join(data_path, file_path)) as file:
        lines = list([line.strip() for line in file.readlines()])

    return [wkt.loads(line) for line in lines]