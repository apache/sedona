<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

Sedona SQL 函数可以通过类似于 Spark 函数的 DataFrame 风格 API 使用。

下列对象包含了对外暴露的函数：`org.apache.spark.sql.sedona_sql.expressions.st_functions`、`org.apache.spark.sql.sedona_sql.expressions.st_constructors`、`org.apache.spark.sql.sedona_sql.expressions.st_predicates` 以及 `org.apache.spark.sql.sedona_sql.expressions.st_aggregates`。

每个函数都可以接收全为 `Column` 类型的参数。此外，重载形式通常还可以接收 `String` 和其他 Scala 类型（例如 `Double`）的混合参数。

一般而言适用以下规则（不过具体函数请参考各自的文档以了解可能的例外）：

=== "Scala"
	1. 每个函数都返回一个 `Column`，因此可以与 Spark 函数以及 `DataFrame` 的方法（如 `DataFrame.select` 或 `DataFrame.join`）互换使用。
	2. 每个函数都有一种全部以 `Column` 作为参数的形式。
	这种形式是最通用的。
	3. 大多数函数都有一种以 `String` 参数与其他 Scala 类型混合使用的形式。

=== "Python"

	1. `Column` 类型的参数会被原样透传，并始终被接受。
	2. `str` 类型的参数总是被视为列名，并会自动包装到 `Column` 中。
	如果需要传递一个实际的字符串字面量，则需要使用 `pyspark.sql.functions.lit` 把它包装成 `Column`。
	3. 其他类型的参数会按函数逐一检查。一般来说，能够合理对应 python 原生类型的参数会被接受并透传。	4. 目前所有函数都不接受 Shapely 的 `Geometry` 对象。

允许的参数类型组合视具体函数而定。
不过在这些情况下，所有 `String` 参数都会被视为列名，并自动包装成 `Column`。
非 `String` 参数会被视为字面量并直接传给对应的 sedona 函数。如果你需要传递 `String` 类型的字面量，应使用 sedona 函数的全 `Column` 形式，并通过 Spark 的 `lit` 函数将该 `String` 字面量包装成 `Column`。

下面是使用该 API 的一个简短示例（用到了 Spark 的 `array_min` 和 `array_max` 函数）：

=== "Scala"

	```scala
	val values_df = spark.sql("SELECT array(0.0, 1.0, 2.0) AS values")
	val min_value = array_min("values")
	val max_value = array_max("values")
	val point_df = values_df.select(ST_Point(min_value, max_value).as("point"))
	```

=== "Python"

	```python3
	from pyspark.sql import functions as f

	from sedona.spark import *

	df = spark.sql("SELECT array(0.0, 1.0, 2.0) AS values")

	min_value = f.array_min("values")
	max_value = f.array_max("values")

	df = df.select(ST_Point(min_value, max_value).alias("point"))
	```

上面的代码将生成如下 dataframe：

```
+-----------+
|point      |
+-----------+
|POINT (0 2)|
+-----------+
```

某些函数会接受 python 的原生值并将其推断为字面量。
例如：

```python3
from sedona.spark import *

df = df.select(ST_Point(1.0, 3.0).alias("point"))
```

这会生成一个包含常量点的列：

```
+-----------+
|point      |
+-----------+
|POINT (1 3)|
+-----------+
```
