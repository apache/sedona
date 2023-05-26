FROM base-jdk

COPY requirements.txt /opt/requirements.txt

RUN apt-get update -y && \
    apt-get install -y python3-pip && \
    pip3 install -r /opt/requirements.txt

# -- PySpark

COPY sedonajars-1.4.0 /usr/local/lib/python3.9/dist-packages/pyspark/jars/

# -- Runtime

EXPOSE 8888

WORKDIR ${SHARED_WORKSPACE}

CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=