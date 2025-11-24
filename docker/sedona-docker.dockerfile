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

FROM ubuntu:24.04

ARG shared_workspace=/opt/workspace
ARG spark_version=4.0.1
ARG hadoop_s3_version=3.4.1
ARG aws_sdk_version=2.38.2
ARG sedona_version=1.8.0
ARG geotools_wrapper_version=1.8.1-33.1
ARG spark_extension_version=2.14.2
ARG zeppelin_version=0.12.0

# Set up envs
ENV SHARED_WORKSPACE=${shared_workspace}
ENV SPARK_HOME=/opt/spark
ENV SEDONA_HOME=/opt/sedona
ENV ZEPPELIN_HOME=/opt/zeppelin
RUN mkdir ${SPARK_HOME}
RUN mkdir ${SEDONA_HOME}

ENV SPARK_MASTER_HOST=localhost
ENV SPARK_MASTER_PORT=7077
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=jupyter
ENV PYTHONPATH=${SPARK_HOME}/python

# Set up OS libraries and PySpark
RUN apt-get update
RUN apt-get install -y openjdk-17-jdk-headless curl python3-pip maven
RUN pip3 install pipenv --break-system-packages
COPY ./docker/install-spark.sh ${SEDONA_HOME}/docker/
RUN chmod +x ${SEDONA_HOME}/docker/install-spark.sh
RUN ${SEDONA_HOME}/docker/install-spark.sh ${spark_version} ${hadoop_s3_version} ${aws_sdk_version}

# Install Python dependencies
COPY docker/requirements.txt /opt/requirements.txt
RUN pip3 install -r /opt/requirements.txt --break-system-packages


# Copy local compiled jars and python code to the docker environment
COPY ./spark-shaded/ ${SEDONA_HOME}/spark-shaded/
COPY ./python/ ${SEDONA_HOME}/python/
# Install Sedona
COPY ./docker/install-sedona.sh ${SEDONA_HOME}/docker/
RUN chmod +x ${SEDONA_HOME}/docker/install-sedona.sh
RUN ${SEDONA_HOME}/docker/install-sedona.sh ${sedona_version} ${geotools_wrapper_version} ${spark_version} ${spark_extension_version}


# Install Zeppelin
COPY ./docker/install-zeppelin.sh ${SEDONA_HOME}/docker/
RUN chmod +x ${SEDONA_HOME}/docker/install-zeppelin.sh
RUN ${SEDONA_HOME}/docker/install-zeppelin.sh ${zeppelin_version} /opt
COPY ./docker/zeppelin/ ${SEDONA_HOME}/docker/zeppelin/

# Set up Zeppelin configuration
COPY docker/zeppelin/conf/zeppelin-site.xml ${ZEPPELIN_HOME}/conf/
COPY docker/zeppelin/conf/helium.json ${ZEPPELIN_HOME}/conf/
COPY docker/zeppelin/conf/interpreter.json ${ZEPPELIN_HOME}/conf/

# Extract version from the actual JAR file and update interpreter.json
COPY ./docker/zeppelin/update-zeppelin-interpreter.sh ${SEDONA_HOME}/docker/zeppelin/
RUN chmod +x ${SEDONA_HOME}/docker/zeppelin/update-zeppelin-interpreter.sh
RUN ${SEDONA_HOME}/docker/zeppelin/update-zeppelin-interpreter.sh

RUN mkdir ${ZEPPELIN_HOME}/helium
RUN mkdir ${ZEPPELIN_HOME}/leaflet
RUN mkdir ${ZEPPELIN_HOME}/notebook/sedona-tutorial
COPY zeppelin/ ${ZEPPELIN_HOME}/leaflet
COPY docker/zeppelin/conf/sedona-zeppelin.json ${ZEPPELIN_HOME}/helium/
COPY docker/zeppelin/examples/*.zpln ${ZEPPELIN_HOME}/notebook/sedona-tutorial/

COPY docs/usecases/*.ipynb /opt/workspace/examples/
COPY docs/usecases/*.py /opt/workspace/examples/
COPY docs/usecases/data /opt/workspace/examples/data

RUN rm -rf ${SEDONA_HOME}

EXPOSE 8888
EXPOSE 8080
EXPOSE 8081
EXPOSE 4040
EXPOSE 8085

WORKDIR ${SHARED_WORKSPACE}

COPY ./docker/start.sh /opt/
CMD ["/bin/bash", "/opt/start.sh"]
