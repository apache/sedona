import warnings

try:
    from pyflink.table import EnvironmentSettings, StreamTableEnvironment
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.java_gateway import get_gateway
except ImportError:
    StreamTableEnvironment = None
    StreamExecutionEnvironment = None
    EnvironmentSettings = None
    warnings.warn(
        "Apache Sedona requires Pyflink. Please install PyFlink before using Sedona flink.",
        DeprecationWarning,
        stacklevel=2,
    )


class SedonaContext:

    @classmethod
    def create(
        cls, env: StreamExecutionEnvironment, settings: EnvironmentSettings
    ) -> StreamTableEnvironment:
        table_env = StreamTableEnvironment.create(env, settings)
        gateway = get_gateway()

        flink_sedona_context = gateway.jvm.org.apache.sedona.flink.SedonaContext

        table_env_j = flink_sedona_context.create(
            env._j_stream_execution_environment, table_env._j_tenv
        )

        table_env._j_tenv = table_env_j

        return table_env
