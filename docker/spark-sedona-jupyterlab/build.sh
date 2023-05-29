#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# -- Software Stack Version

PYTHON_VERSION="3.9"
SPARK_VERSION="3.3.2"
HADOOP_VERSION="3"
SEDONA_VERSION="1.4.0"
GEOTOOLS_WRAPPER_VERSION="1.4.0-28.2"

# -- Building the Images

docker build \
    --no-cache \
    --build-arg python_version="${PYTHON_VERSION}" \
    -f base-jdk.dockerfile \
    -t base-jdk .

docker build \
    --no-cache \
    --build-arg spark_version="${SPARK_VERSION}" \
    --build-arg hadoop_version="${HADOOP_VERSION}" \
    --build-arg sedona_version="${SEDONA_VERSION}" \
    --build-arg geotools_wrapper_version="${GEOTOOLS_WRAPPER_VERSION}" \
    -f spark-base.dockerfile \
    -t spark-base .

docker build \
    --no-cache \
    -f spark-master.dockerfile \
    -t spark-master .

docker build \
    --no-cache \
    -f spark-worker.dockerfile \
    -t spark-worker .

docker build \
    --no-cache \
    --build-arg sedona_version="${SEDONA_VERSION}" \
    --build-arg geotools_wrapper_version="${GEOTOOLS_WRAPPER_VERSION}" \
    -f jupyterlab.dockerfile \
    -t sedona_jupyterlab:1.4.0 .