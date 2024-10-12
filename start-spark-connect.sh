SPARK_VERSION=3.5.1
HADOOP_VERSION=3
spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/sbin/start-connect-server.sh \
--packages org.apache.spark:spark-connect_2.12:${SPARK_VERSION},org.datasyslab:geotools-wrapper:1.6.1-28.2 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.kryo.registrator=org.apache.sedona.core.serde.SedonaKryoRegistrator \
--conf spark.sql.extensions=org.apache.sedona.sql.SedonaSqlExtensions
