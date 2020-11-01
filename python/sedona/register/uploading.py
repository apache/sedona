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
from os import path

import findspark


def find_spark_version() -> str:
    from pyspark.version import __version__
    major_version = __version__.split(".")[:-1]
    return "_".join(major_version)


def get_abs_path() -> str:
    return path.abspath(path.dirname(__file__))


def get_module_path(abs_path: str) -> str:
    return os.path.join(*os.path.split(abs_path)[:-1])


def create_jars_path(module_path: str, spark_version: str) -> str:
    return os.path.join(module_path, "jars", spark_version)


def upload_jars_based_on_spark_version(module_path: str) -> bool:
    spark_version = find_spark_version()
    jars_path = create_jars_path(module_path, spark_version)
    if spark_version == "2_2":
        findspark.add_jars([os.path.join(jars_path, file) for file in os.listdir(jars_path)])
    else:
        findspark.add_jars(os.path.join(jars_path, "*"))

    return True


def upload_jars() -> bool:
    module_path = get_module_path(get_abs_path())
    upload_jars_based_on_spark_version(module_path)
    findspark.init()
    return True
