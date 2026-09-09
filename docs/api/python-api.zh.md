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

# Python API 参考

[Python API 参考文档](pydocs/index.html)介绍了 Sedona 的 Python 模块、类和函数，包括参数和返回类型。该参考文档为英文版。

要了解在 PySpark DataFrame 中使用的函数，请参阅 [SQL 函数参考](pydocs/sedona.spark.sql.html)。例如，[ST_Length](pydocs/sedona.spark.sql.html#sedona.spark.sql.st_functions.ST_Length) 可以计算线几何对象的长度：

```python
from sedona.spark.sql.st_functions import ST_Length

df.select(ST_Length("geometry").alias("length"))
```

此示例假设已有名为 `df` 的 DataFrame，其中包含名为 `geometry` 的几何列，并且 Spark 会话已配置 Sedona。有关配置步骤，请参阅 [Python 安装指南](../setup/install-python.md)；有关参数约定和更多示例，请参阅 [DataFrame API 指南](sql/DataFrameAPI.md)。
