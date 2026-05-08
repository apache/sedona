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

要在 Apache Sedona 中配置 PyFlink，请先按 [PyFlink](../../setup/flink/install-python.md) 指南完成安装。
完成后，可以运行下面的代码以验证环境是否正常工作。

```python
from sedona.flink import SedonaContext
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

stream_env = StreamExecutionEnvironment.get_execution_environment()
flink_settings = EnvironmentSettings.in_streaming_mode()
table_env = SedonaContext.create(stream_env, flink_settings)

table_env.sql_query("SELECT ST_Point(1.0, 2.0)").execute()
```

PyFlink 不支持把 Scala 自定义类型（UDT）转换为 Python UDT。
因此，如果想在 Python 中收集结果，需要使用 `ST_AsText` 或 `ST_ASBinary` 等函数把结果转换为字符串或二进制。

```python
from shapely.wkb import loads

table_env.sql_query("SELECT ST_ASBinary(ST_Point(1.0, 2.0))").execute().collect()

[loads(bytes(el[0])) for el in result]
```

```
[<POINT (1 2)>]
```

用户自定义标量函数（UDF）也是类似的处理方式：

```python
from pyflink.table.udf import ScalarFunction, udf
from shapely.wkb import loads


class Buffer(ScalarFunction):
    def eval(self, s):
        geom = loads(s)
        return geom.buffer(1).wkb


table_env.create_temporary_function(
    "ST_BufferPython", udf(Buffer(), result_type="Binary")
)

buffer_table = table_env.sql_query(
    "SELECT ST_BufferPython(ST_ASBinary(ST_Point(1.0, 2.0))) AS buffer"
)
```

更多 SQL 示例请参阅 FlinkSQL 章节：[FlinkSQL](sql.md)。
