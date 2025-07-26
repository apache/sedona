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

set -e

# Define variables
spark_version=$1
hadoop_s3_version=$2
aws_sdk_version=$3
spark_xml_version=$4

# Validate inputs
if [[ -z "$spark_version" || -z "$hadoop_s3_version" || -z "$aws_sdk_version" || -z "$spark_xml_version" ]]; then
  echo "Usage: $0 <spark_version> <hadoop_s3_version> <aws_sdk_version> <spark_xml_version>"
  exit 1
fi

# Download Spark jar and set up PySpark
curl --retry 5 --retry-delay 10 --retry-connrefused "https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop3.tgz" -o spark.tgz
tar -xf spark.tgz && mv "spark-${spark_version}-bin-hadoop3"/* "${SPARK_HOME}/"
rm spark.tgz
rm -rf "spark-${spark_version}-bin-hadoop3"

# Add S3 jars
curl --retry 5 --retry-delay 10 --retry-connrefused "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${hadoop_s3_version}/hadoop-aws-${hadoop_s3_version}.jar" -o "${SPARK_HOME}/jars/hadoop-aws-${hadoop_s3_version}.jar"
curl --retry 5 --retry-delay 10 --retry-connrefused "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${aws_sdk_version}/aws-java-sdk-bundle-${aws_sdk_version}.jar" -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-${aws_sdk_version}.jar"

# Add spark-xml jar
curl --retry 5 --retry-delay 10 --retry-connrefused "https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/${spark_xml_version}/spark-xml_2.12-${spark_xml_version}.jar" -o "${SPARK_HOME}/jars/spark-xml_2.12-${spark_xml_version}.jar"

# Set up master IP address and executor memory
cp "${SPARK_HOME}/conf/spark-defaults.conf.template" "${SPARK_HOME}/conf/spark-defaults.conf"

# Install required libraries for GeoPandas on Apple chip mac
# Note: 'apt-get' is for Debian/Ubuntu, check if running on Linux
if command -v apt-get &>/dev/null; then
  apt-get update
  apt-get install -y gdal-bin libgdal-dev openssh-client openssh-server
  systemctl enable ssh
elif [[ "$(uname)" == "Darwin" ]]; then
  echo "Please install gdal and openssh with Homebrew:"
  echo "  brew install gdal openssh"
else
  echo "Unsupported OS for automatic package installation. Please install gdal and openssh manually."
fi

# Enable passwordless SSH
if [[ ! -f ~/.ssh/id_rsa ]]; then
  ssh-keygen -t rsa -f ~/.ssh/id_rsa -N ""
fi
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
