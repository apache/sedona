# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
