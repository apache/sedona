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

FROM junhao/base-jdk

# -- Layer: Apache Spark

ARG spark_version=3.3.2
ARG hadoop_version=3
ARG sedona_version=1.4.1
ARG geotools_wrapper_version=1.4.0-28.2

ENV SPARK_HOME /opt/spark
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYTHONPATH=$SPARK_HOME/python
ENV PYSPARK_PYTHON python3
ENV PYSPARK_DRIVER_PYTHON jupyter

RUN apt-get update -y && \
    apt-get install -y curl && \
    curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} $SPARK_HOME && \
    mkdir /opt/spark/logs && \
    rm spark.tgz
    # -- Copy sedona jars to Spark jars
RUN curl https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.0_2.12/1.4.1/sedona-spark-shaded-3.0_2.12-1.4.1.jar -o sedona-spark-shaded-3.0_2.12-1.4.1.jar && \
    # tar -xf spark.tgz && \
    mv sedona-spark-shaded-3.0_2.12-1.4.1.jar ${SPARK_HOME}/jars/ && \
    # -- Copy geotools-wrapper jars to Spark jars
    curl https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/${geotools_wrapper_version}/geotools-wrapper-${geotools_wrapper_version}.jar -o geotools-wrapper-${geotools_wrapper_version}.jar && \
    mv geotools-wrapper-${geotools_wrapper_version}.jar ${SPARK_HOME}/jars/

# -- Runtime

WORKDIR ${SPARK_HOME}