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

import pytest


def test_sedonadb_connect():
    pytest.importorskip("sedonadb")

    import sedona.db

    sd = sedona.db.connect()
    assert sd.sql("SELECT 1 as one").count() == 1


def test_sedonadb_dbapi():
    pytest.importorskip("sedonadb")
    pytest.importorskip("adbc_driver_manager")

    import sedona.db.dbapi

    with sedona.db.dbapi.connect() as con, con.cursor() as cur:
        cur.execute("SELECT 1 as one")
        assert cur.fetchall() == [(1,)]


def test_sedonadb_testing():
    pytest.importorskip("sedonadb")

    from sedona.db.testing import SedonaDB

    eng = SedonaDB()
    eng.assert_query_result("SELECT 1 as one", "1")
