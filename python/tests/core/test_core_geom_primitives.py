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

from sedona.spark.core.geom.envelope import Envelope


class TestGeomPrimitives(TestBase):

    def test_jvm_envelope(self):
        envelope = Envelope(0.0, 5.0, 0.0, 5.0)
        jvm_instance = envelope.create_jvm_instance(self.spark.sparkContext._jvm)
        envelope_area = jvm_instance.getArea()
        assert (
            envelope_area == 25.0
        ), f"Expected area to be equal 25 but {envelope_area} was found"
