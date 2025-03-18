#!/bin/bash

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
set -e

# Define Zeppelin version and target directory
ZEPPELIN_VERSION=${1:-0.9.0}
TARGET_DIR=${2:-/opt}

# Download and extract Zeppelin using curl
curl -L https://archive.apache.org/dist/zeppelin/zeppelin-"${ZEPPELIN_VERSION}"/zeppelin-"${ZEPPELIN_VERSION}"-bin-all.tgz \
    -o zeppelin-"${ZEPPELIN_VERSION}"-bin-all.tgz
tar -xzf zeppelin-"${ZEPPELIN_VERSION}"-bin-all.tgz -C "${TARGET_DIR}"
mv "${TARGET_DIR}"/zeppelin-"${ZEPPELIN_VERSION}"-bin-all "${ZEPPELIN_HOME}"
rm zeppelin-"${ZEPPELIN_VERSION}"-bin-all.tgz
