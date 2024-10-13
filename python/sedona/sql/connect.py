from typing import Union, Any, Iterable, Tuple

from pyspark.sql.connect.column import Column
from pyspark.sql.connect.expressions import CallFunction
import pyspark.sql.connect.functions as f


class SedonaFunction(CallFunction):
    def __repr__(self):
        if len(self._args) > 0:
            return f"{self._name}({', '.join([str(arg) for arg in self._args])})"
        else:
            return f"{self._name}()"


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


def call_sedona_function_connect(
    function_name: str, args: Tuple[Any]
) -> Column:

    expressions = [_convert_argument_to_connect_column(arg)._expr for arg in args]
    return Column(SedonaFunction(function_name, expressions))
