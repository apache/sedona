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

SPARK_VERSION="3.4.1"
HADOOP_VERSION="3"
SEDONA_VERSION="1.4.1"
GEOTOOLS_WRAPPER_VERSION="1.4.0-28.2"

lower_version=$(echo -e $SPARK_VERSION"\n3.4" | sort -V | head -n1)
if [ $lower_version = "3.4" ]; then
    SEDONA_SPARK_VERSION=3.4
else
    SEDONA_SPARK_VERSION=3.0
fi

# -- Building the Images

docker build \
    --progress=plain \
    --build-arg spark_version="${SPARK_VERSION}" \
    --build-arg hadoop_version="${HADOOP_VERSION}" \
    -f docker/spark-base.dockerfile \
    -t sedona/spark-base:latest \
    -t sedona/spark-base:${SPARK_VERSION} .

docker build \
    --progress=plain \
    --build-arg spark_version="${SPARK_VERSION}" \
    --build-arg sedona_version="${SEDONA_VERSION}" \
    --build-arg geotools_wrapper_version="${GEOTOOLS_WRAPPER_VERSION}" \
    --build-arg sedona_spark_version="${SEDONA_SPARK_VERSION}" \
    -f docker/sedona-release.dockerfile \
    -t sedona/sedona-release:latest \
    -t sedona/sedona-release:${SEDONA_VERSION} .

docker build \
    --progress=plain \
    --build-arg sedona_version="${SEDONA_VERSION}" \
    -f docker/spark-sedona-jupyterlab/sedona_jupyterlab.dockerfile \
    -t sedona/sedona_jupyterlab:latest \
    -t sedona/sedona_jupyterlab:${SEDONA_VERSION} .