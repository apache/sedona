from typing import Any, Tuple, Union, Iterable
from pyspark.sql import SparkSession, Column, functions as f


ColumnOrName = Union[Column, str]
ColumnOrNameOrNumber = Union[Column, str, float, int]


def _convert_argument_to_java_column(arg: Any) -> Column:
    if isinstance(arg, Column):
        return arg._jc
    elif isinstance(arg, str):
        return f.col(arg)._jc
    elif isinstance(arg, Iterable):
        return f.array(*[Column(x) for x in map(_convert_argument_to_java_column, arg)])._jc
    else:
        return f.lit(arg)._jc


def call_sedona_function(object_name: str, function_name: str, args: Union[Any, Tuple[Any]]) -> Column:
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise ValueError("No active spark session was detected. Unable to call sedona function.")
    
    if not isinstance(args, Iterable) or isinstance(args, str):
        args = (args,)

    args = map(_convert_argument_to_java_column, args)

    jobject = getattr(spark._jvm, object_name)
    jfunc = getattr(jobject, function_name)
    
    jc = jfunc(*args)
    return Column(jc)
        