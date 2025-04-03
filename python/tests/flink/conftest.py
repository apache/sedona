import os

import pytest


EXTRA_JARS = os.getenv("SEDONA_PYFLINK_EXTRA_JARS")


def has_pyflink():
    try:
        import pyflink
    except ImportError:
        return False
    return True


if has_pyflink():
    from sedona.flink import SedonaContext
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.table import EnvironmentSettings, StreamTableEnvironment

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
