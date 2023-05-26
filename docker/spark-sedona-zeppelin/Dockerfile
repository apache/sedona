FROM apache/zeppelin:0.10.1

EXPOSE 8080

USER root

WORKDIR /opt/zeppelin/

ENV SPARK_HOME=/opt/zeppelin/spark

ENV SPARK_MASTER=spark://spark-master:7077

ENV PYTHONPATH=$SPARK_HOME/python

COPY requirements.txt /opt/requirements.txt

RUN python3 -m pip install -r /opt/requirements.txt

COPY jars ./jars

COPY zeppelin/ /opt/zeppelin/

RUN addgroup --gid 1000 zeppelin 

RUN adduser --gecos '' --uid 1000 --gid 1000 --shell /bin/bash --no-create-home --disabled-password zeppelin

# RUN mkdir -p $SPARK_HOME/jars

RUN chown -R zeppelin:zeppelin .

RUN chmod -R 775 .

# RUN chmod -R 777 $SPARK_HOME

USER 1000:1000

CMD \
    # chown -R zeppelin:zeppelin ./spark && \
    mv ./jars/* /$SPARK_HOME/jars/ && \
    rm -r ./jars && \
    /opt/zeppelin/bin/zeppelin.sh

# ENTRYPOINT exec su root && \
#     cp -a ./jars/ /$SPARK_HOME/jars/ && \
#     rm -r ./jars && \
#     su 1000 && \
#     /opt/zeppelin/bin/zeppelin.sh