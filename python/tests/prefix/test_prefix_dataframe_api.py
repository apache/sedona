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
import threading

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F
from tests.test_base import SPARK_REMOTE, TestBase

from sedona.spark.sql import st_aggregates as sta
from sedona.spark.sql import st_constructors as stc
from sedona.spark.sql import st_functions as stf
from sedona.spark.sql import st_predicates as stp
from sedona.spark.sql._function_names import (
    PREFIX_CONFIG_KEY,
    call_sedona_function_by_name,
    function_prefix,
    resolve_function_name,
)

# The prefix is JVM-wide and fixed by the first Sedona registrator, so these tests need a
# pytest process of their own whose session was built with SEDONA_TEST_FUNCTION_PREFIX.
PREFIX = os.getenv("SEDONA_TEST_FUNCTION_PREFIX")
pytestmark = pytest.mark.skipif(not PREFIX, reason="needs SEDONA_TEST_FUNCTION_PREFIX")


class TestPrefixDataFrameApi(TestBase):
    def test_prefix_is_visible_to_python(self):
        assert function_prefix(self.spark) == PREFIX
        assert resolve_function_name("ST_Point", self.spark) == PREFIX + "ST_Point"

    def test_prefixed_sql(self):
        row = self.spark.sql(
            f"SELECT {PREFIX}ST_AsText({PREFIX}ST_Point(1.0, 2.0))"
        ).head()
        assert row[0] == "POINT (1 2)"

    def test_plain_sedona_name_is_not_registered(self):
        # Classic and Spark Connect both raise a subclass of this AnalysisException.
        with pytest.raises(
            AnalysisException, match="UNRESOLVED_ROUTINE.*`ST_SubDivideExplode`"
        ):
            self.spark.sql("SELECT ST_SubDivideExplode(null, 5)").collect()

    def test_scalar_wrappers(self):
        df = self.spark.range(1).select(
            stf.ST_AsText(stc.ST_Point(F.lit(1.0), F.lit(2.0)))
        )
        assert df.head()[0] == "POINT (1 2)"

    def test_predicate_wrapper(self):
        df = self.spark.range(1).select(
            stp.ST_Intersects(
                stc.ST_Point(F.lit(1.0), F.lit(2.0)),
                stc.ST_Point(F.lit(1.0), F.lit(2.0)),
            )
        )
        assert df.head()[0] is True

    def test_aggregate_wrapper(self):
        points = self.spark.sql(
            f"SELECT {PREFIX}ST_Point(x, x) AS geom FROM VALUES (0.0), (1.0) AS t(x)"
        )
        assert (
            points.select(stf.ST_NumGeometries(sta.ST_Union_Aggr("geom"))).head()[0]
            == 2
        )

    @pytest.mark.skipif(not SPARK_REMOTE, reason="needs Spark Connect")
    def test_second_connect_session_asking_for_another_prefix(self):
        from pyspark.sql.connect.session import SparkSession as ConnectSession

        self.spark  # starts the server for SPARK_REMOTE=local
        # The local server that SPARK_REMOTE=local starts listens at sc://localhost.
        url = "sc://localhost" if SPARK_REMOTE.startswith("local") else SPARK_REMOTE
        outcome = {}

        def build_and_run():
            # A new thread has no active session, so the session created here becomes the
            # active one in this thread, and the wrappers resolve the prefix through it.
            second = ConnectSession.builder.remote(url).create()
            try:
                second.conf.set(PREFIX_CONFIG_KEY, "other_")
                outcome["wkt"] = (
                    second.range(1)
                    .select(stf.ST_AsText(stc.ST_Point(1.0, 2.0)))
                    .head()[0]
                )
            except Exception as error:
                outcome["error"] = error
            finally:
                # Not stop(): in local mode that also stops the server.
                second.client.close()

        thread = threading.Thread(target=build_and_run)
        thread.start()
        thread.join()
        if "error" in outcome:
            raise outcome["error"]
        assert outcome["wkt"] == "POINT (1 2)"

    def test_by_name_helper(self):
        df = self.spark.range(1).select(
            call_sedona_function_by_name(
                "ST_AsText",
                call_sedona_function_by_name("ST_Point", F.lit(1.0), F.lit(2.0)),
            )
        )
        assert df.head()[0] == "POINT (1 2)"
