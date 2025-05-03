#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from typing import Any, Iterable, List

import pyspark.sql.connect.functions as f
from pyspark.sql.connect.column import Column
from pyspark.sql.connect.expressions import UnresolvedFunction


# mimic semantics of _convert_argument_to_java_column
def _convert_argument_to_connect_column(arg: Any) -> Column:
    if isinstance(arg, Column):
        return arg
    elif isinstance(arg, str):
        return f.col(arg)
    elif isinstance(arg, Iterable):
        return f.array(*[_convert_argument_to_connect_column(x) for x in arg])
    else:
        return f.lit(arg)


def call_sedona_function_connect(function_name: str, args: List[Any]) -> Column:

    expressions = [_convert_argument_to_connect_column(arg)._expr for arg in args]
    return Column(UnresolvedFunction(function_name, expressions))
