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

ARG spark_version=3.3.2
FROM sedona/spark-base:${spark_version}

# -- Layer: Apache Spark
ARG sedona_version=1.4.1
ARG geotools_wrapper_version=1.4.0-28.2
ARG sedona_spark_version=3.0

# Download Sedona
RUN curl https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-${sedona_spark_version}_2.12/${sedona_version}/sedona-spark-shaded-${sedona_spark_version}_2.12-${sedona_version}.jar -o $SPARK_HOME/jars/sedona-spark-shaded-${sedona_spark_version}_2.12-${sedona_version}.jar
RUN curl https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/${geotools_wrapper_version}/geotools-wrapper-${geotools_wrapper_version}.jar -o $SPARK_HOME/jars/geotools-wrapper-${geotools_wrapper_version}.jar 

# Install Sedona Python
RUN pip3 install shapely==1.8.4
RUN pip3 install apache-sedona==${sedona_version}

# -- Runtime

WORKDIR ${SPARK_HOME}