import logging

try:
    from sedona.flink.context import SedonaContext

    __all__ = ["SedonaContext"]
except ImportError:
    logging.log(
        logging.WARN,
        "SedonaContext could not be imported. This is likely due to a missing flink dependency.",
    )
    __all__ = []
