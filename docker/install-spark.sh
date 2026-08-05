#!/bin/bash

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

set -e

# Define variables
hadoop_s3_version=$1
aws_sdk_version=$2

# Helper function to download with throttled progress updates (every 5 seconds)
download_with_progress() {
    local url=$1
    local output=$2
    local description=${3:-"Downloading"}

    # Start download in background, redirect progress to /dev/null
    curl -L --fail --silent --show-error --retry 5 --retry-delay 10 --retry-connrefused "${url}" -o "${output}" &
    local curl_pid=$!

    # Monitor progress every 5 seconds
    while kill -0 $curl_pid 2>/dev/null; do
        sleep 5
        if [ -f "${output}" ]; then
            # Use stat for portability (works on both Linux and macOS)
            local size=$(stat -c%s "${output}" 2>/dev/null || stat -f%z "${output}" 2>/dev/null || echo 0)
            local size_mb=$((size / 1024 / 1024))
            echo "${description}... ${size_mb} MB downloaded"
        fi
    done

    # Wait for curl to finish
    wait $curl_pid
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        echo "Download failed with exit code $exit_code"
        return $exit_code
    fi

    # Show final size
    if [ -f "${output}" ]; then
        local final_size=$(stat -c%s "${output}" 2>/dev/null || stat -f%z "${output}" 2>/dev/null || echo 0)
        local final_size_mb=$((final_size / 1024 / 1024))
        echo "${description} completed: ${final_size_mb} MB"
    fi
}

# Spark itself is provided by the official apache/spark image via a multi-stage
# COPY in sedona-docker.dockerfile. That image ships without a conf directory,
# so create it here. The tgz distribution's conf templates only contain
# commented-out examples, so empty files are equivalent; start.sh appends the
# actual settings at container startup.
mkdir -p "${SPARK_HOME}"/conf
touch "${SPARK_HOME}"/conf/spark-defaults.conf
touch "${SPARK_HOME}"/conf/spark-env.sh.template

# Add S3 jars
echo "Downloading Hadoop AWS S3 jar..."
download_with_progress "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${hadoop_s3_version}/hadoop-aws-${hadoop_s3_version}.jar" "${SPARK_HOME}/jars/hadoop-aws-${hadoop_s3_version}.jar" "Downloading Hadoop AWS"

# Add AWS SDK v2 bundle (required by spark-extension 2.14.2+)
echo "Downloading AWS SDK v2 bundle..."
download_with_progress "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${aws_sdk_version}/bundle-${aws_sdk_version}.jar" "${SPARK_HOME}/jars/aws-sdk-v2-bundle-${aws_sdk_version}.jar" "Downloading AWS SDK"

# Install required libraries for GeoPandas on Apple chip mac
apt-get install -y gdal-bin libgdal-dev

# Install OpenSSH for cluster mode
apt-get install -y openssh-client openssh-server
systemctl enable ssh

# Enable nopassword ssh
ssh-keygen -t rsa -f ~/.ssh/id_rsa -N ""
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
