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

from sedona.core.jvm.config import is_greater_or_equal_version, SedonaMeta
from tests.test_base import TestBase


class TestGeoSparkMeta(TestBase):

    def test_meta(self):
        assert not is_greater_or_equal_version("1.1.5", "1.2.0")
        assert is_greater_or_equal_version("1.2.0", "1.1.5")
        assert is_greater_or_equal_version("1.3.5", "1.2.0")
        assert not is_greater_or_equal_version("", "1.2.0")
        assert not is_greater_or_equal_version("1.3.5", "")
        SedonaMeta.version = "1.2.0"
        assert SedonaMeta.version == "1.2.0"
        SedonaMeta.version = "1.3.0"
        assert SedonaMeta.version == "1.3.0"
