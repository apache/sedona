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

from pyspark import RDD, SparkContext

from sedona.spark.core.jvm.translate import JvmSedonaPythonConverter
from sedona.spark.utils.spatial_rdd_parser import SedonaPickler


class SedonaRDD:

    def __init__(self, jsrdd, sc: SparkContext):
        self.jsrdd = jsrdd
        self.sc = sc

    def to_rdd(self) -> RDD:
        jvm = self.sc._jvm
        serialized = JvmSedonaPythonConverter(jvm).translate_spatial_rdd_to_python(
            self.jsrdd
        )

        return RDD(serialized, self.sc, SedonaPickler())


class SedonaPairRDD:

    def __init__(self, jsrdd, sc: SparkContext):
        self.jsrdd = jsrdd
        self.sc = sc

    def to_rdd(self) -> RDD:
        jvm = self.sc._jvm
        serialized = JvmSedonaPythonConverter(jvm).translate_spatial_pair_rdd_to_python(
            self.jsrdd
        )

        return RDD(serialized, self.sc, SedonaPickler())


class SedonaPairRDDList:

    def __init__(self, jsrdd, sc: SparkContext):
        self.jsrdd = jsrdd
        self.sc = sc

    def to_rdd(self):
        jvm = self.sc._jvm
        serialized = JvmSedonaPythonConverter(
            jvm
        ).translate_spatial_pair_rdd_with_list_to_python(self.jsrdd)

        return RDD(serialized, self.sc, SedonaPickler())
