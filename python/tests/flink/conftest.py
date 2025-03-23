import os

import pytest

try:
    from sedona.flink import SedonaContext
except ImportError:
    pass

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


EXTRA_JARS = os.getenv("SEDONA_PYFLINK_EXTRA_JARS")


@pytest.fixture(scope="module")
def flink_settings():
    return EnvironmentSettings.in_streaming_mode()


@pytest.fixture(scope="module")
def stream_env() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    jars = EXTRA_JARS.split(",") if EXTRA_JARS else []
    for jar in jars:
        env.add_jars(f"file://{jar}")

    return env


@pytest.fixture(scope="module")
def table_env(
    stream_env: StreamExecutionEnvironment, flink_settings: EnvironmentSettings
) -> StreamTableEnvironment:
    return SedonaContext.create(stream_env, flink_settings)
