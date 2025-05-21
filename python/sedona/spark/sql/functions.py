# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import inspect
from enum import Enum

import pandas as pd

from sedona.spark.sql.types import GeometryType
from sedona.spark.utils import geometry_serde
from pyspark.sql.udf import UserDefinedFunction
from pyspark.sql.types import DataType
from shapely.geometry.base import BaseGeometry


SEDONA_SCALAR_EVAL_TYPE = 5200
SEDONA_PANDAS_ARROW_NAME = "SedonaPandasArrowUDF"


class SedonaUDFType(Enum):
    SHAPELY_SCALAR = "ShapelyScalar"
    GEO_SERIES = "GeoSeries"


class InvalidSedonaUDFType(Exception):
    pass


sedona_udf_to_eval_type = {
    SedonaUDFType.SHAPELY_SCALAR: SEDONA_SCALAR_EVAL_TYPE,
    SedonaUDFType.GEO_SERIES: SEDONA_SCALAR_EVAL_TYPE,
}


def sedona_vectorized_udf(
    return_type: DataType, udf_type: SedonaUDFType = SedonaUDFType.SHAPELY_SCALAR
):
    import geopandas as gpd

    def apply_fn(fn):
        function_signature = inspect.signature(fn)
        serialize_geom = False
        deserialize_geom = False

        if isinstance(return_type, GeometryType):
            serialize_geom = True

        if issubclass(function_signature.return_annotation, BaseGeometry):
            serialize_geom = True

        if issubclass(function_signature.return_annotation, gpd.GeoSeries):
            serialize_geom = True

        for param in function_signature.parameters.values():
            if issubclass(param.annotation, BaseGeometry):
                deserialize_geom = True

            if issubclass(param.annotation, gpd.GeoSeries):
                deserialize_geom = True

        if udf_type == SedonaUDFType.SHAPELY_SCALAR:
            return _apply_shapely_series_udf(
                fn, return_type, serialize_geom, deserialize_geom
            )

        if udf_type == SedonaUDFType.GEO_SERIES:
            return _apply_geo_series_udf(
                fn, return_type, serialize_geom, deserialize_geom
            )

        raise InvalidSedonaUDFType(f"Invalid UDF type: {udf_type}")

    return apply_fn


def _apply_shapely_series_udf(
    fn, return_type: DataType, serialize_geom: bool, deserialize_geom: bool
):
    def apply(series: pd.Series) -> pd.Series:
        applied = series.apply(
            lambda x: (
                fn(geometry_serde.deserialize(x)[0]) if deserialize_geom else fn(x)
            )
        )

        return applied.apply(
            lambda x: geometry_serde.serialize(x) if serialize_geom else x
        )

    udf = UserDefinedFunction(
        apply, return_type, "SedonaPandasArrowUDF", evalType=SEDONA_SCALAR_EVAL_TYPE
    )

    return udf


def _apply_geo_series_udf(
    fn, return_type: DataType, serialize_geom: bool, deserialize_geom: bool
):
    import geopandas as gpd

    def apply(series: pd.Series) -> pd.Series:
        series_data = series
        if deserialize_geom:
            series_data = gpd.GeoSeries(
                series.apply(lambda x: geometry_serde.deserialize(x)[0])
            )

        return fn(series_data).apply(
            lambda x: geometry_serde.serialize(x) if serialize_geom else x
        )

    return UserDefinedFunction(
        apply, return_type, "SedonaPandasArrowUDF", evalType=SEDONA_SCALAR_EVAL_TYPE
    )


def deserialize_geometry_if_geom(data):
    if isinstance(data, BaseGeometry):
        return geometry_serde.deserialize(data)[0]

    return data


def serialize_to_geometry_if_geom(data, return_type: DataType):
    if isinstance(return_type, GeometryType):
        return geometry_serde.serialize(data)

    return data
