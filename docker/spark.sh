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
spark_version=$1
hadoop_s3_version=$2
aws_sdk_version=$3
spark_xml_version=$4

# Set up OS libraries
apt-get update
apt-get install -y openjdk-19-jdk-headless curl python3-pip maven
pip3 install --upgrade pip && pip3 install pipenv

# Download Spark jar and set up PySpark
pip3 install pyspark=="${spark_version}"

# Add S3 jars
curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/"${hadoop_s3_version}"/hadoop-aws-"${hadoop_s3_version}".jar -o "${SPARK_HOME}"/jars/hadoop-aws-"${hadoop_s3_version}".jar
curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/"${aws_sdk_version}"/aws-java-sdk-bundle-"${aws_sdk_version}".jar -o "${SPARK_HOME}"/jars/aws-java-sdk-bundle-"${aws_sdk_version}".jar

# Add spark-xml jar
curl https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/"${spark_xml_version}"/spark-xml_2.12-"${spark_xml_version}".jar -o "${SPARK_HOME}"/jars/spark-xml_2.12-"${spark_xml_version}".jar

# Install required libraries for GeoPandas on Apple chip mac
apt-get install -y gdal-bin libgdal-dev

# Install OpenSSH for cluster mode
apt-get install -y openssh-client openssh-server
systemctl enable ssh

# Enable nopassword ssh
ssh-keygen -t rsa -f ~/.ssh/id_rsa -N ""
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
