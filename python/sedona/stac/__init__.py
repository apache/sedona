from sedona.spark.stac.client import Client

import warnings

warnings.warn(
    "The 'sedona.stac' module is deprecated and will be removed in future versions. Please use 'sedona.spark.stac' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "Client",
]
