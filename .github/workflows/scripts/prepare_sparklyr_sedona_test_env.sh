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

sedona_jar_files () {
  local subdir
  for subdir in 'core' 'sql' 'viz'; do
    local artifact_id="$(
      mvn \
        org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate \
        -Dexpression=project.artifactId \
        -q \
        -DforceStdout \
        -f "${subdir}"/target/resolved-pom.xml
    )"
    local artifact_version="$(
      mvn \
        org.apache.maven.plugins:maven-help-plugin:3.2.0:evaluate \
        -Dexpression=project.version \
        -q \
        -DforceStdout \
        -f "${subdir}"/target/resolved-pom.xml
    )"
    echo "$(pwd)/${subdir}/target/${artifact_id}-${artifact_version}.jar"
  done
}

export SEDONA_JAR_FILES="$(sedona_jar_files | paste -sd':')"
