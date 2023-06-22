FROM apache/zeppelin:0.10.1

ARG sedona_version=1.3.1-incubating
ARG geotools_wrapper_version=1.3.0-27.2

USER root

EXPOSE 8080

ENV SPARK_HOME=/opt/spark
ENV SPARK_MASTER=spark://spark-master:7077
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH=${SPARK_HOME}/python
ENV PATH=${PYTHONPATH}:${PATH}

COPY docker/spark-sedona-zeppelin/zeppelin/ /opt/zeppelin/
RUN mkdir -p /opt/zeppelin/sedona_mapviz_widget
COPY zeppelin/index.js /opt/zeppelin/sedona_mapviz_widget
COPY zeppelin/package.json /opt/zeppelin/sedona_mapviz_widget
COPY docker/spark-sedona-zeppelin/requirements.txt /opt/requirements.txt
COPY binder/* /opt/workspace/examples/

RUN pip3 install --upgrade pip
RUN pip3 install -r /opt/requirements.txt && \
    rm /opt/requirements.txt

WORKDIR /opt/zeppelin

# -- Runtime
CMD ["/opt/zeppelin/bin/zeppelin.sh"]