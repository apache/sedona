from enum import Enum

import pandas as pd

from sedona.sql.types import GeometryType
from sedona.utils import geometry_serde
from pyspark.sql.udf import UserDefinedFunction

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


def sedona_vectorized_udf(udf_type: SedonaUDFType = SedonaUDFType.SHAPELY_SCALAR):
    def apply_fn(fn):
        if udf_type == SedonaUDFType.SHAPELY_SCALAR:
            return _apply_shapely_series_udf(fn)

        if udf_type == SedonaUDFType.GEO_SERIES:
            return _apply_geo_series_udf(fn)

        raise InvalidSedonaUDFType(f"Invalid UDF type: {udf_type}")

    return apply_fn


def _apply_shapely_series_udf(fn):
    def apply(series: pd.Series) -> pd.Series:
        applied = series.apply(lambda x: fn(geometry_serde.deserialize(x)[0]))

        return applied.apply(lambda x: geometry_serde.serialize(x))

    return UserDefinedFunction(
        apply, GeometryType(), "SedonaPandasArrowUDF", evalType=SEDONA_SCALAR_EVAL_TYPE
    )


def _apply_geo_series_udf(fn):
    import geopandas as gpd

    def apply(series: pd.Series) -> pd.Series:
        geo_series = gpd.GeoSeries(
            series.apply(lambda x: geometry_serde.deserialize(x)[0])
        )

        return fn(geo_series).apply(lambda x: geometry_serde.serialize(x))

    return UserDefinedFunction(
        apply, GeometryType(), "SedonaPandasArrowUDF", evalType=SEDONA_SCALAR_EVAL_TYPE
    )
