from sedona.spark.stats.outlier_detection.local_outlier_factor import (
    local_outlier_factor,
)

import warnings

warnings.warn(
    "The 'sedona.stats.outlier_detection.local_outlier_factor' module is deprecated and will be removed in future versions. Please use 'sedona.spark.stats' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["local_outlier_factor"]
