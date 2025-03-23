from sedona.spark.stats.weighting import add_binary_distance_band_column

import warnings

warnings.warn(
    "The 'sedona.stats.weighting' module is deprecated and will be removed in future versions. Please use 'sedona.spark.stats' instead.",
    DeprecationWarning,
    stacklevel=2,
)
