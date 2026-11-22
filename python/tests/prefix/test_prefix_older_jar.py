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

"""function_prefix on classic when the JVM's Sedona jar has no SedonaFunctionNames.

These tests need no JVM and no SEDONA_TEST_FUNCTION_PREFIX.
"""

from types import SimpleNamespace

import pytest
from py4j.java_gateway import JavaPackage
from py4j.protocol import Py4JError, Py4JNetworkError

from sedona.spark.sql import _function_names
from sedona.spark.sql._function_names import function_prefix, resolve_function_name


class _Lookup:
    """Stands in for spark._jvm: any attribute path ends at ``leaf``, or raises ``error``."""

    def __init__(self, leaf=None, error=None):
        self._leaf = leaf
        self._error = error

    def __getattr__(self, name):
        if name == "effectivePrefix":
            if self._error is not None:
                raise self._error
            return self._leaf
        return self


def _session(lookup):
    return SimpleNamespace(_jvm=lookup)


@pytest.fixture(autouse=True)
def classic(monkeypatch):
    monkeypatch.setattr(_function_names, "is_remote", lambda: False)


def test_jar_without_function_names_has_no_prefix():
    # What py4j returns for a class the JVM does not have: calling it raises
    # TypeError: 'JavaPackage' object is not callable.
    package = JavaPackage(
        "org.apache.sedona.sql.UDF.SedonaFunctionNames.effectivePrefix", None
    )
    with pytest.raises(TypeError, match="not callable"):
        package()
    spark = _session(_Lookup(leaf=package))
    assert function_prefix(spark) == ""
    assert resolve_function_name("ST_KNN", spark) == "ST_KNN"


def test_lookup_error_means_no_prefix():
    spark = _session(
        _Lookup(error=Py4JError("effectivePrefix does not exist in the JVM"))
    )
    assert function_prefix(spark) == ""


def test_jvm_prefix_is_returned():
    spark = _session(_Lookup(leaf=lambda: "sedona_"))
    assert function_prefix(spark) == "sedona_"


def test_other_errors_propagate():
    with pytest.raises(Py4JNetworkError):
        function_prefix(_session(_Lookup(error=Py4JNetworkError("gateway closed"))))

    def fail():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        function_prefix(_session(_Lookup(leaf=fail)))
