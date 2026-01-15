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

from sedona.spark.utils import geometry_serde
from shapely.geometry.base import BaseGeometry
from pyspark.sql.udf import UserDefinedFunction
from sedona.spark.sql.types import GeometryType
from pyspark.sql.types import (
    DataType,
    FloatType,
    DoubleType,
    IntegerType,
    StringType,
    ByteType,
)

from sedona.spark.utils.udf import has_sedona_serializer_speedup

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


def infer_pa_type(spark_type: DataType):
    import pyarrow as pa
    import geoarrow.pyarrow as ga

    if isinstance(spark_type, GeometryType):
        return ga.wkb()
    elif isinstance(spark_type, FloatType):
        return pa.float32()
    elif isinstance(spark_type, DoubleType):
        return pa.float64()
    elif isinstance(spark_type, IntegerType):
        return pa.int32()
    elif isinstance(spark_type, StringType):
        return pa.string()
    else:
        raise NotImplementedError(f"Type {spark_type} is not supported yet.")


def infer_input_type(spark_type: DataType):
    from sedonadb import udf as sedona_udf_module

    if isinstance(spark_type, GeometryType):
        return sedona_udf_module.GEOMETRY
    elif (
        isinstance(spark_type, FloatType)
        or isinstance(spark_type, DoubleType)
        or isinstance(spark_type, IntegerType)
    ):
        return sedona_udf_module.NUMERIC
    elif isinstance(spark_type, StringType):
        return sedona_udf_module.STRING
    elif isinstance(spark_type, ByteType):
        return sedona_udf_module.BINARY
    else:
        raise NotImplementedError(f"Type {spark_type} is not supported yet.")


def infer_input_types(spark_types: list[DataType]):
    pa_types = []
    for spark_type in spark_types:
        pa_type = infer_input_type(spark_type)
        pa_types.append(pa_type)

    return pa_types


def sedona_db_vectorized_udf(
    return_type: DataType,
    input_types: list[DataType],
):
    from sedonadb import udf as sedona_udf_module

    eval_type = 6200
    if has_sedona_serializer_speedup():
        eval_type = 6201

    def apply_fn(fn):
        out_type = infer_pa_type(return_type)
        input_types_sedona_db = infer_input_types(input_types)

        @sedona_udf_module.arrow_udf(out_type, input_types=input_types_sedona_db)
        def shapely_udf(*args, **kwargs):
            return fn(*args, **kwargs)

        udf = UserDefinedFunction(
            lambda: shapely_udf, return_type, "SedonaPandasArrowUDF", evalType=eval_type
        )

        return udf

    return apply_fn
