#!/bin/sh

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

apt-get install -y jq

# Find the latest Sedona and GeoTools JARs
SEDONA_JAR=$(find "$SPARK_HOME/jars/" -name "sedona-spark-shaded-*.jar" | head -n 1)
GEOTOOLS_JAR=$(find "$SPARK_HOME/jars/" -name "geotools-wrapper-*.jar" | head -n 1)

# Ensure we found the jars
if [ -z "$SEDONA_JAR" ] || [ -z "$GEOTOOLS_JAR" ]; then
    echo "Error: One or both JARs not found."
    exit 1
fi

# Update interpreter.json using jq
jq --arg sedona "$SEDONA_JAR" --arg geotools "$GEOTOOLS_JAR" '
  .interpreterSettings.spark.dependencies |= map(
    if .groupArtifactVersion | test("sedona-spark-shaded-.*\\.jar$") then
      .groupArtifactVersion = $sedona
    elif .groupArtifactVersion | test("geotools-wrapper-.*\\.jar$") then
      .groupArtifactVersion = $geotools
    else . end
  )
' "$ZEPPELIN_HOME/conf/interpreter.json" > "$ZEPPELIN_HOME/conf/interpreter.json.tmp" && \
mv "$ZEPPELIN_HOME/conf/interpreter.json.tmp" "$ZEPPELIN_HOME/conf/interpreter.json"

echo "Updated interpreter.json successfully!"
