from sedona.spark.stats.clustering.dbscan import dbscan

import warnings

warnings.warn(
    "The 'sedona.spark.stats.clustering.dbscan' module is deprecated and will be removed in future versions. Please use 'sedona.spark.stats' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["dbscan"]
