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

# Define variables
sedona_version=$1
geotools_wrapper_version=$2
spark_version=$3

lower_version=$(echo -e $spark_version"\n3.4" | sort -V | head -n1)
if [ $lower_version = "3.4" ]; then
    sedona_spark_version=3.4
else
    sedona_spark_version=3.0
fi

if [ $sedona_version = "latest" ]; then
    # Code to execute when SEDONA_VERSION is "latest"
    cp ${SEDONA_HOME}/spark-shaded/target/sedona-spark-shaded-*.jar ${SPARK_HOME}/jars/
	cd ${SEDONA_HOME}/python;pip3 install shapely==1.8.4;pip3 install .
else
    # Code to execute when SEDONA_VERSION is not "latest"
    # Download Sedona
	curl https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-${sedona_spark_version}_2.12/${sedona_version}/sedona-spark-shaded-${sedona_spark_version}_2.12-${sedona_version}.jar -o $SPARK_HOME/jars/sedona-spark-shaded-${sedona_spark_version}_2.12-${sedona_version}.jar
	curl https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/${geotools_wrapper_version}/geotools-wrapper-${geotools_wrapper_version}.jar -o $SPARK_HOME/jars/geotools-wrapper-${geotools_wrapper_version}.jar

	# Install Sedona Python
	pip3 install shapely==1.8.4
	pip3 install apache-sedona==${sedona_version}
fi
