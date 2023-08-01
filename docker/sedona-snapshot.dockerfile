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
ARG sedona_spark_version=3.0


ADD spark-shaded/target/sedona-spark-shaded-*.jar $SPARK_HOME/jars/

# Install Sedona Python
RUN mkdir /opt/sedona
RUN mkdir /opt/sedona/python
COPY ./python /opt/sedona/python/
WORKDIR /opt/sedona/python
RUN pip3 install shapely==1.8.4
RUN pip3 install .

# -- Runtime

WORKDIR ${SPARK_HOME}