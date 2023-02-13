# Example of spark + sedona + hdfs with slave nodes and OSM vector data consults

```
from IPython.display import display, HTML
from pyspark.sql import SparkSession
from pyspark import StorageLevel
import pandas as pd
from pyspark.sql.types import StructType, StructField,StringType, LongType, IntegerType, DoubleType, ArrayType
from pyspark.sql.functions import regexp_replace
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from pyspark.sql.functions import col, split, expr
from pyspark.sql.functions import udf, lit
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
from pyspark.sql.functions import col, split, expr
from pyspark.sql.functions import udf, lit, flatten
from pywebhdfs.webhdfs import PyWebHdfsClient
from datetime import date
from pyspark.sql.functions import monotonically_increasing_id 
import json
```

### Registering spark session, adding node executor configurations and sedona registrator

```
spark = SparkSession.\
    builder.\
    appName("Overpass-API").\
    enableHiveSupport().\
    master("local[*]").\
    master("spark://spark-master:7077").\
    config("spark.executor.memory", "15G").\
    config("spark.driver.maxResultSize", "135G").\
    config("spark.sql.shuffle.partitions", "500").\
    config(' spark.sql.adaptive.coalescePartitions.enabled', True).\
    config('spark.sql.adaptive.enabled', True).\
    config('spark.sql.adaptive.coalescePartitions.initialPartitionNum', 125).\
    config("spark.sql.execution.arrow.pyspark.enabled", True).\
    config("spark.sql.execution.arrow.fallback.enabled", True).\
    config('spark.kryoserializer.buffer.max', 2047).\
    config("spark.serializer", KryoSerializer.getName).\
    config("spark.kryo.registrator", SedonaKryoRegistrator.getName).\
    config("spark.jars.packages", "org.apache.sedona:sedona-python-adapter-3.0_2.12:1.1.0-incubating,org.datasyslab:geotools-wrapper:1.1.0-25.2") .\
    enableHiveSupport().\
    getOrCreate()

SedonaRegistrator.registerAll(spark)
sc = spark.sparkContext
```

### Connecting to Overpass API to search and downloading data for saving into HDFS 

```
import requests
import json

overpass_url = "http://overpass-api.de/api/interpreter"
overpass_query = """
[out:json];
area[name = "Foz do Iguaçu"];
way(area)["highway"~""];
out geom;
>;
out skel qt;
"""

response = requests.get(overpass_url, 
                         params={'data': overpass_query})
data = response.json()
hdfs = PyWebHdfsClient(host='179.106.229.159',port='50070', user_name='root')
file_name = "foz_roads_osm.json"
hdfs.delete_file_dir(file_name)
hdfs.create_file(file_name, json.dumps(data))

```

### Connecting spark sedona with saved hdfs file

```
path = "hdfs://776faf4d6a1e:8020/"+file_name
df = spark.read.json(path, multiLine = "true")
```

### Consulting and organizing data for analysis

```
from pyspark.sql.functions import explode, arrays_zip

df.createOrReplaceTempView("df")
tb = spark.sql("select *, size(elements) total_nodes from df")
tb.show(5)

isolate_total_nodes = tb.select("total_nodes").toPandas()
total_nodes = isolate_total_nodes["total_nodes"].iloc[0]
print(total_nodes)

isolate_ids = tb.select("elements.id").toPandas()
ids = pd.DataFrame(isolate_ids["id"].iloc[0]).drop_duplicates()
print(ids[0].iloc[1])

formatted_df = tb\
.withColumn("id", explode("elements.id"))

formatted_df.show(5)

formatted_df = tb\
.withColumn("new", arrays_zip("elements.id", "elements.geometry", "elements.nodes", "elements.tags"))\
.withColumn("new", explode("new"))

formatted_df.show(5)

# formatted_df.printSchema()

formatted_df = formatted_df.select("new.0","new.1","new.2","new.3.maxspeed","new.3.incline","new.3.surface", "new.3.name", "total_nodes")
formatted_df = formatted_df.withColumnRenamed("0","id").withColumnRenamed("1","geom").withColumnRenamed("2","nodes").withColumnRenamed("3","tags")
formatted_df.createOrReplaceTempView("formatted_df")
formatted_df.show(5)
# TODO atualizar daqui para baixo para considerar a linha inteira na lógica
points_tb = spark.sql("select geom, id from formatted_df where geom IS NOT NULL")
points_tb = points_tb\
.withColumn("new", arrays_zip("geom.lat", "geom.lon"))\
.withColumn("new", explode("new"))

points_tb = points_tb.select("new.0","new.1", "id")

points_tb = points_tb.withColumnRenamed("0","lat").withColumnRenamed("1","lon")
points_tb.printSchema()

points_tb.createOrReplaceTempView("points_tb")

points_tb.show(5)

coordinates_tb = spark.sql("select (select collect_list(CONCAT(p1.lat,',',p1.lon)) from points_tb p1 where p1.id = p2.id group by p1.id) as coordinates, p2.id, p2.maxspeed, p2.incline, p2.surface, p2.name, p2.nodes, p2.total_nodes from formatted_df p2")
coordinates_tb.createOrReplaceTempView("coordinates_tb")
coordinates_tb.show(5)

roads_tb = spark.sql("SELECT ST_LineStringFromText(REPLACE(REPLACE(CAST(coordinates as string),'[',''),']',''), ',') as geom, id, maxspeed, incline, surface, name, nodes, total_nodes FROM coordinates_tb WHERE coordinates IS NOT NULL")
roads_tb.createOrReplaceTempView("roads_tb")
roads_tb.show(5)
```
