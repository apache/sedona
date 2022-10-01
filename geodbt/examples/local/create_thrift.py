from pyspark.sql import SparkSession
from py4j.java_gateway import java_import
import time
 
spark = SparkSession.builder \
  .appName("Embedding Spark Thrift Server") \
  .config("spark.sql.hive.thriftServer.singleSession", "True") \
  .config("hive.server2.thrift.port", "10002") \
  .config("javax.jdo.option.ConnectionURL",\
  "jdbc:derby:;databaseName=metastore_db2;create=true") \
  .enableHiveSupport() \
  .getOrCreate() 

sc = spark.sparkContext
jvm = sc._gateway.jvm
spark.sql("CREATE DATABASE IF NOT EXISTS sedona").show()
#
java_import(jvm, "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2")
java_import(jvm, "org.apache.sedona.sql.utils.SedonaSQLRegistrator")

# Start Spark Thrift Server using the jvm and passing the SparkSession


jvm.HiveThriftServer2.startWithContext(spark._jsparkSession.sqlContext())
jvm.SedonaSQLRegistrator.registerAll(spark._jsparkSession)

while True:
  time.sleep(5)