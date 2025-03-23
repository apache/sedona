import warnings

from sedona.spark.geoarrow.geoarrow import create_spatial_dataframe

warnings.warn(
    "The 'sedona.utils.geoarrow' module is deprecated and will be removed in future versions. Please use 'sedona.spark.geoarrow' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["create_spatial_dataframe"]
