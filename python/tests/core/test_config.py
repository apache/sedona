#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import os

from sedona.core.jvm.config import SparkJars, SedonaMeta
from tests.test_base import TestBase


class TestCoreJVMConfig(TestBase):
    def test_yarn_jars(self):

        used_jar_files = ",".join(
            os.listdir(os.path.join(os.environ["SPARK_HOME"], "jars"))
        )

        # Test's don't run on YARN but can set the property manually to confirm
        # the python checks work
        self.spark.conf.set("spark.yarn.dist.jars", used_jar_files)

        assert "sedona" in SparkJars().jars

        self.spark.conf.unset("spark.yarn.dist.jars")

    def test_sedona_version(self):
        used_jar_files = ",".join(
            os.listdir(os.path.join(os.environ["SPARK_HOME"], "jars"))
        )

        # Test's don't run on YARN but can set the property manually to confirm
        # the python checks work
        self.spark.conf.set("spark.yarn.dist.jars", used_jar_files)

        assert SedonaMeta().version is not None

        self.spark.conf.unset("spark.yarn.dist.jars")
