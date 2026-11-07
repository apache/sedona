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

from sedona.spark.sql.types import (
    GeometryType,
    GeographyType,
    USES_NATIVE_SPATIAL_TYPES,
    to_shapely,
    to_spark_geometry,
    to_spark_geography,
)
from sedona.spark.utils.geometry_sql_serde import (
    deserialize_geometry,
    serialize_geometry,
)
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

        if USES_NATIVE_SPATIAL_TYPES:
            return _apply_native_series_udf(fn, return_type, udf_type, deserialize_geom)

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


def _apply_native_series_udf(fn, return_type, udf_type, deserialize_geom):
    """Use Spark's pandas UDF transport for its own native spatial values."""
    import geopandas as gpd
    from pyspark.sql.functions import pandas_udf, PandasUDFType

    if udf_type not in (SedonaUDFType.SHAPELY_SCALAR, SedonaUDFType.GEO_SERIES):
        raise InvalidSedonaUDFType(f"Invalid UDF type: {udf_type}")

    def apply(series):
        values = series.map(to_shapely) if deserialize_geom else series
        if udf_type == SedonaUDFType.GEO_SERIES:
            output = fn(gpd.GeoSeries(values) if deserialize_geom else values)
        else:
            output = values.map(fn)
        if isinstance(return_type, (GeometryType, GeographyType)):
            convert = (
                to_spark_geometry
                if isinstance(return_type, GeometryType)
                else to_spark_geography
            )
            # Empty/all-null GeoSeries retain geometry dtype after map(). Spark
            # native values need an ordinary object Series at its Arrow boundary.
            return pd.Series(
                [convert(value) for value in output], index=output.index, dtype=object
            )
        return output

    return pandas_udf(apply, return_type, PandasUDFType.SCALAR)


def _apply_shapely_series_udf(
    fn, return_type: DataType, serialize_geom: bool, deserialize_geom: bool
):
    def apply(series: pd.Series) -> pd.Series:
        applied = series.apply(
            lambda x: (
                fn(deserialize_geometry(x))
                if deserialize_geom and x is not None
                else fn(x)
            )
        )

        return applied.apply(lambda x: serialize_geometry(x) if serialize_geom else x)

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
                series.apply(
                    lambda x: (deserialize_geometry(x) if x is not None else None)
                )
            )

        result = fn(series_data)
        if serialize_geom:
            # GeoSeries.apply retains geometry dtype for empty or all-null
            # results, which Arrow cannot convert to the binary storage type.
            return pd.Series(
                [serialize_geometry(geom) for geom in result],
                index=result.index,
                dtype=object,
            )
        return result

    return UserDefinedFunction(
        apply, return_type, "SedonaPandasArrowUDF", evalType=SEDONA_SCALAR_EVAL_TYPE
    )


def deserialize_geometry_if_geom(data):
    if isinstance(data, BaseGeometry):
        return deserialize_geometry(data)

    return data


def serialize_to_geometry_if_geom(data, return_type: DataType):
    if isinstance(return_type, GeometryType):
        return serialize_geometry(data)

    return data
