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

# Define Zeppelin version and target directory
ZEPPELIN_VERSION=${1:-0.9.0}
TARGET_DIR=${2:-/opt}

# Helper function to download with throttled progress updates (every 5 seconds)
download_with_progress() {
    local url=$1
    local output=$2
    local description=${3:-"Downloading"}

    # Start download in background
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

# Download and extract Zeppelin
echo "Downloading Zeppelin ${ZEPPELIN_VERSION}..."
zeppelin_filename="zeppelin-${ZEPPELIN_VERSION}-bin-netinst.tgz"
download_with_progress "https://archive.apache.org/dist/zeppelin/zeppelin-${ZEPPELIN_VERSION}/${zeppelin_filename}" "${zeppelin_filename}" "Downloading Zeppelin"
tar -xzf "${zeppelin_filename}" -C "${TARGET_DIR}"
mv "${TARGET_DIR}"/zeppelin-"${ZEPPELIN_VERSION}"-bin-netinst "${ZEPPELIN_HOME}"
rm "${zeppelin_filename}"
