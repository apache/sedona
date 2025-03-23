import logging
import warnings

try:
    import pyflink
    from sedona.flink.context import SedonaContext
except ImportError:
    warnings.warn(
        "Apache Sedona requires Pyflink. Please install PyFlink before using Sedona flink.",
        DeprecationWarning,
        stacklevel=2,
    )

__all__ = ["SedonaContext"]
