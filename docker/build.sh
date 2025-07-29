#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at

#   http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

SPARK_VERSION=$1
SEDONA_VERSION=$2
BUILD_MODE=$3
GEOTOOLS_VERSION=${4:-auto}

SEDONA_SPARK_VERSION=${SPARK_VERSION:0:3}
if [ "${SPARK_VERSION:0:1}" -eq "3" ] && [ "${SPARK_VERSION:2:1}" -le "3" ]; then
    # 3.0, 3.1, 3.2, 3.3
    SEDONA_SPARK_VERSION=3.0
fi

# Function to compare two version numbers
version_gt() {
  # Compare two version numbers
  # Returns 0 if the first version is greater, 1 otherwise
  [ "$(printf '%s\n' "$@" | sort -V | head -n 1)" != "$1" ]
}

# Function to get the latest version of a Maven package
get_latest_version_with_suffix() {
  BASE_URL=$1
  SUFFIX=$2

  # Fetch the maven-metadata.xml file
  METADATA_URL="${BASE_URL}maven-metadata.xml"
  METADATA_XML=$(curl -s "$METADATA_URL")

  # Extract versions from the XML
  VERSIONS=$(echo "$METADATA_XML" | grep -o '<version>[^<]*</version>' | awk -F'[<>]' '{print $3}')

  LATEST_VERSION=""

  # Filter versions that end with the specified suffix and find the largest one
  for VERSION in $VERSIONS; do
    if [[ $VERSION == *$SUFFIX ]]; then
      if [[ -z $LATEST_VERSION ]] || version_gt "$VERSION" "$LATEST_VERSION"; then
        LATEST_VERSION=$VERSION
      fi
    fi
  done

  if [[ -z $LATEST_VERSION ]]; then
    exit 1
  else
    echo "$LATEST_VERSION"
  fi
}

if [ "$GEOTOOLS_VERSION" = "auto" ]; then
    GEOTOOLS_VERSION=$(mvn help:evaluate -Dexpression=geotools.version -q -DforceStdout)
    echo "GeoTools version inferred from pom.xml: $GEOTOOLS_VERSION"
fi

GEOTOOLS_WRAPPER_VERSION="${SEDONA_VERSION}-${GEOTOOLS_VERSION}"
if [ "$SEDONA_VERSION" = "latest" ]; then
    GEOTOOLS_WRAPPER_VERSION=$(get_latest_version_with_suffix "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/" "$GEOTOOLS_VERSION")
    if [ -z "$GEOTOOLS_WRAPPER_VERSION" ]; then
        echo "No geotools-wrapper version with suffix $GEOTOOLS_VERSION"
        exit 1
    fi
    echo "Using latest geotools-wrapper version: $GEOTOOLS_WRAPPER_VERSION"

    # The compilation must take place outside Docker to avoid unnecessary maven packages
    mvn clean install -DskipTests -Dspark="${SEDONA_SPARK_VERSION}" -Dscala=2.12
fi

# -- Building the image

if [ -z "$BUILD_MODE" ] || [ "$BUILD_MODE" = "local" ]; then
    # If local, build the image for the local environment
    docker buildx build --load \
    --build-arg spark_version="${SPARK_VERSION}" \
    --build-arg sedona_version="${SEDONA_VERSION}" \
    --build-arg geotools_wrapper_version="${GEOTOOLS_WRAPPER_VERSION}" \
    -f docker/sedona-docker.dockerfile \
    -t apache/sedona:"${SEDONA_VERSION}" .
else
    # If release, build the image for cross-platform
    docker buildx build --platform linux/amd64,linux/arm64 \
    --progress=plain \
    --no-cache \
    --output type=registry \
    --build-arg spark_version="${SPARK_VERSION}" \
    --build-arg sedona_version="${SEDONA_VERSION}" \
    --build-arg geotools_wrapper_version="${GEOTOOLS_WRAPPER_VERSION}" \
    -f docker/sedona-docker.dockerfile \
    -t apache/sedona:"${SEDONA_VERSION}" .
fi
