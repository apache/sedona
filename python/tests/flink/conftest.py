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

    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.table import EnvironmentSettings, StreamTableEnvironment
    except ImportError:
        pytest.skip("PyFlink is not installed. Skipping tests that require PyFlink.")

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
