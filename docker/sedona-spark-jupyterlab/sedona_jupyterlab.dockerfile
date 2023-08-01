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

ARG sedona_version=1.4.1
FROM sedona/sedona:${sedona_version}

# Install Python dependencies
COPY docker/sedona-spark-jupyterlab/requirements.txt /opt/requirements.txt
RUN pip3 install -r /opt/requirements.txt

COPY binder/*.ipynb /opt/workspace/examples/
COPY binder/*.py /opt/workspace/examples/
COPY binder/data /opt/workspace/examples/data

# Add the master IP address to all notebooks
RUN find /opt/workspace/examples/ -type f -name "*.ipynb" -exec sed -i 's/config = SedonaContext.builder()/config = SedonaContext.builder().master(\\"spark:\/\/localhost:7077\\")/' {} +
# Delete packages configured by the notebooks
RUN find /opt/workspace/examples/ -type f -name "*.ipynb" -exec sed -i '/spark\.jars\.packages/d' {} +
RUN find /opt/workspace/examples/ -type f -name "*.ipynb" -exec sed -i '/org\.apache\.sedona:sedona-spark-shaded-/d' {} +
RUN find /opt/workspace/examples/ -type f -name "*.ipynb" -exec sed -i '/org\.datasyslab:geotools-wrapper:/d' {} +

EXPOSE 8888

WORKDIR ${SHARED_WORKSPACE}

CMD service ssh start && ${SPARK_HOME}/sbin/start-all.sh && jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=