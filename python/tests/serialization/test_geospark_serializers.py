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

from tests.test_base import TestBase


class TestGeometryConvert(TestBase):

    def test_creating_point(self):
        self.spark.sql("SELECT st_GeomFromWKT('Point(21.0 52.0)')").show()

    def test_spark_config(self):
        kryo_reg = ('spark.kryo.registrator', 'org.apache.sedona.core.serde.SedonaKryoRegistrator')
        serializer = ('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        spark_config = self.spark.sparkContext._conf.getAll()
        assert kryo_reg in spark_config
        assert serializer in spark_config
