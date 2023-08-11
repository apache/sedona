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

SPARK_VERSION=$1
SEDONA_VERSION=$2
BUILD_MODE=$3

lower_version=$(echo -e ${SPARK_VERSION}"\n3.4" | sort -V | head -n1)
if [ $lower_version = "3.4" ]; then
    SEDONA_SPARK_VERSION=3.4
else
    SEDONA_SPARK_VERSION=3.0
fi

if [ "$SEDONA_VERSION" = "latest" ]; then
    # The compilation must take place outside Docker to avoid unnecessary maven packages
    mvn clean install -DskipTests  -Dspark=${SEDONA_SPARK_VERSION} -Dgeotools -Dscala=2.12
fi

# -- Building the image

if [ -z "$BUILD_MODE" ] || [ "$BUILD_MODE" = "local" ]; then
    # If local, build the image for the local environment
    docker build \
    --build-arg spark_version="${SPARK_VERSION}" \
    --build-arg sedona_version="${SEDONA_VERSION}" \
    -f docker/sedona-spark-jupyterlab/sedona-jupyterlab.dockerfile \
    -t apache/sedona:${SEDONA_VERSION} .
else
    # If release, build the image for cross-platform
    docker buildx build --platform linux/amd64,linux/arm64 \
    --progress=plain \
    --no-cache \
    --output type=registry \
    --build-arg spark_version="${SPARK_VERSION}" \
    --build-arg sedona_version="${SEDONA_VERSION}" \
    -f docker/sedona-spark-jupyterlab/sedona-jupyterlab.dockerfile \
    -t apache/sedona:${SEDONA_VERSION} .
fi