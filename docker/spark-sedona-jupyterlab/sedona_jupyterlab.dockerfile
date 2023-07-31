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

ARG python_version=3.9
RUN python_version=`echo ${python_version} | cut -d '.' -f 1-2`
ARG sedona_version=1.4.1
ARG geotools_wrapper_version=1.4.0-28.2

COPY docker/spark-sedona-jupyterlab/requirements.txt /opt/requirements.txt
COPY binder/* /opt/workspace/examples/

RUN apt-get update -y && \
    apt-get install -y python3-pip curl && \
    pip3 install --upgrade pip
RUN pip3 install -r /opt/requirements.txt
RUN curl https://dlcdn.apache.org/sedona/${sedona_version}/apache-sedona-${sedona_version}-bin.tar.gz -o sedona.tar.gz && \
    tar -xf sedona.tar.gz && \
    # -- Copy sedona jars to PySpark jars
    mv apache-sedona-${sedona_version}-bin/* /usr/local/lib/python${python_version}/dist-packages/pyspark/jars/ && \
    rm sedona.tar.gz && \
    # -- Copy geotools-wrapper jars to PySpark jars
    curl https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/${geotools_wrapper_version}/geotools-wrapper-${geotools_wrapper_version}.jar -o geotools-wrapper-${geotools_wrapper_version}.jar && \
    mv geotools-wrapper-${geotools_wrapper_version}.jar /usr/local/lib/python${python_version}/dist-packages/pyspark/jars/ 

# -- Runtime

EXPOSE 8888

WORKDIR ${SHARED_WORKSPACE}

CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=