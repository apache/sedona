from sedona.spark.utils.structured_adapter import StructuredAdapter

import warnings

warnings.warn(
    "The 'sedona.utils.structured_adapter' module is deprecated and will be removed in future versions. Please use 'sedona.spark' instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "StructuredAdapter",
]
