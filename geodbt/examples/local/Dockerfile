FROM openjdk:8
ENV SPARK_VERSION=3.3.0
ENV HADOOP_VERSION=3
ENV SCALA_VERSION=2.12
ENV SEDONA_VERSION=1.2.1
ENV SPARK_COMPATIBLE_VERSION=3.0
ENV SEDONA_ADAPTER_PYTHON_VERSION=${SPARK_COMPATIBLE_VERSION}_${SCALA_VERSION}-${SEDONA_VERSION}
ENV GEOTOOLS_WRAPPER_VERSION=1.1.0-25.2

RUN apt-get update &&  apt-get install wget && \
    wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    wget https://repo1.maven.org/maven2/org/apache/sedona/sedona-python-adapter-${SPARK_COMPATIBLE_VERSION}_${SCALA_VERSION}/${SEDONA_VERSION}-incubating/sedona-python-adapter-${SEDONA_ADAPTER_PYTHON_VERSION}-incubating.jar && \
    wget https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/${GEOTOOLS_WRAPPER_VERSION}/geotools-wrapper-${GEOTOOLS_WRAPPER_VERSION}.jar


COPY create_thrift.py spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/run_thrift.py
RUN cp sedona-python-adapter-${SEDONA_ADAPTER_PYTHON_VERSION}-incubating.jar spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/
RUN cp geotools-wrapper-1.1.0-25.2.jar spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/

EXPOSE 10002
EXPOSE 4040-4070

WORKDIR spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin

CMD ["./spark-submit", "run_thrift.py"]