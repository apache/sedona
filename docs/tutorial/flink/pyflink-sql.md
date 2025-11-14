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

To set up the PyFlink with Apache Sedona, please follow the guide. [PyFlink](../../setup/flink/install-python.md)
When you finish it, you can run the following code to test if everything works.

```python
from sedona.flink import SedonaContext
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

stream_env = StreamExecutionEnvironment.get_execution_environment()
flink_settings = EnvironmentSettings.in_streaming_mode()
table_env = SedonaContext.create(stream_env, flink_settings)

table_env.sql_query("SELECT ST_Point(1.0, 2.0)").execute()
```

PyFlink does not expose the possibility of transforming Scala's own user-defined types (UDT) to Python UDT.
So, when you want to collect the result in Python, you need to use functions
like `ST_AsText` or `ST_ASBinary` to convert the result to a string or binary.

```python
from shapely.wkb import loads

table_env.sql_query("SELECT ST_ASBinary(ST_Point(1.0, 2.0))").execute().collect()

[loads(bytes(el[0])) for el in result]
```

```
[<POINT (1 2)>]
```

Similar with User Defined Scalar functions

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

For more SQL examples please follow the FlinkSQL section [FlinkSQL](sql.md).
