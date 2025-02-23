import pandas as pd

from sedona.sql.types import GeometryType
from sedona.utils import geometry_serde
from pyspark.sql.udf import UserDefinedFunction


SEDONA_SCALAR_EVAL_TYPE = 5200


def sedona_vectorized_udf(fn):
    def apply(series: pd.Series) -> pd.Series:
        geo_series = series.apply(lambda x: fn(geometry_serde.deserialize(x)[0]))

        return geo_series.apply(lambda x: geometry_serde.serialize(x))

    return UserDefinedFunction(
        apply, GeometryType(), "SedonaPandasArrowUDF", evalType=SEDONA_SCALAR_EVAL_TYPE
    )
