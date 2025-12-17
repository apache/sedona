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
spark_version=$1
hadoop_s3_version=$2
aws_sdk_version=$3

# Helper function to download with throttled progress updates (every 5 seconds)
download_with_progress() {
    local url=$1
    local output=$2
    local description=${3:-"Downloading"}

    # Start download in background, redirect progress to /dev/null
    curl -L --silent --show-error --retry 5 --retry-delay 10 --retry-connrefused "${url}" -o "${output}" &
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

# Download Spark jar and set up PySpark
# Download from Lyra Hosting mirror (faster) but verify checksum from Apache archive
spark_filename="spark-${spark_version}-bin-hadoop3.tgz"
spark_download_url="https://mirror.lyrahosting.com/apache/spark/spark-${spark_version}/${spark_filename}"
checksum_url="https://archive.apache.org/dist/spark/spark-${spark_version}/${spark_filename}.sha512"

echo "Downloading Spark ${spark_version} from Lyra Hosting mirror..."
download_with_progress "${spark_download_url}" "${spark_filename}" "Downloading Spark"

echo "Downloading checksum from Apache archive..."
curl -L --silent --show-error --retry 5 --retry-delay 10 --retry-connrefused "${checksum_url}" -o "${spark_filename}.sha512"

echo "Verifying checksum..."
sha512sum -c "${spark_filename}.sha512" || { echo "Checksum verification failed!"; exit 1; }

echo "Checksum verified successfully. Extracting Spark..."
tar -xf "${spark_filename}" && mv spark-"${spark_version}"-bin-hadoop3/* "${SPARK_HOME}"/
rm "${spark_filename}" "${spark_filename}.sha512" && rm -rf spark-"${spark_version}"-bin-hadoop3

# Add S3 jars
echo "Downloading Hadoop AWS S3 jar..."
download_with_progress "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${hadoop_s3_version}/hadoop-aws-${hadoop_s3_version}.jar" "${SPARK_HOME}/jars/hadoop-aws-${hadoop_s3_version}.jar" "Downloading Hadoop AWS"

# Add AWS SDK v2 bundle (required by spark-extension 2.14.2+)
echo "Downloading AWS SDK v2 bundle..."
download_with_progress "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${aws_sdk_version}/bundle-${aws_sdk_version}.jar" "${SPARK_HOME}/jars/aws-sdk-v2-bundle-${aws_sdk_version}.jar" "Downloading AWS SDK"

# Set up master IP address and executor memory
cp "${SPARK_HOME}"/conf/spark-defaults.conf.template "${SPARK_HOME}"/conf/spark-defaults.conf

# Install required libraries for GeoPandas on Apple chip mac
apt-get install -y gdal-bin libgdal-dev

# Install OpenSSH for cluster mode
apt-get install -y openssh-client openssh-server
systemctl enable ssh

# Enable nopassword ssh
ssh-keygen -t rsa -f ~/.ssh/id_rsa -N ""
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
