#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Worker that receives input from Piped RDD.
"""
import logging
import os
import sys
import time
from inspect import currentframe, getframeinfo, getfullargspec
import importlib
import json
from io import BufferedRWPair
from typing import Any, Iterable, Iterator

from sedonaworker.serializer import SedonaArrowStreamPandasUDFSerializer

# 'resource' is a Unix specific module.
has_resource_module = True
try:
    import resource
except ImportError:
    has_resource_module = False
import traceback
import warnings
import faulthandler

from pyspark.accumulators import _accumulatorRegistry
from pyspark.broadcast import Broadcast, _broadcastRegistry
from pyspark.java_gateway import local_connect_and_auth
from pyspark.taskcontext import BarrierTaskContext, TaskContext
from pyspark.files import SparkFiles
from pyspark.resource import ResourceInformation
from pyspark.rdd import PythonEvalType
from pyspark.serializers import (
    write_with_length,
    write_int,
    read_long,
    read_bool,
    write_long,
    read_int,
    SpecialLengths,
    UTF8Deserializer,
    CPickleSerializer,
    BatchedSerializer,
)
from pyspark.sql.pandas.serializers import (
    ArrowStreamPandasUDFSerializer, ArrowStreamSerializer
)
from pyspark.sql.pandas.types import to_arrow_type
from pyspark.sql.types import BinaryType, StringType, StructType, _parse_datatype_json_string
from pyspark.util import fail_on_stopiteration, try_simplify_traceback
from pyspark import shuffle
from pyspark.errors import PySparkRuntimeError, PySparkTypeError

pickleSer = CPickleSerializer()
utf8_deserializer = UTF8Deserializer()


class SedonaArrowStreamUDFSerializer(ArrowStreamSerializer):
    """
    Same as :class:`ArrowStreamSerializer` but it flattens the struct to Arrow record batch
    for applying each function with the raw record arrow batch. See also `DataFrame.mapInArrow`.
    """

    def load_stream(self, stream):
        """
        Flatten the struct into Arrow's record batches.
        """
        import pyarrow as pa

        batches = super(SedonaArrowStreamUDFSerializer, self).load_stream(stream)
        for batch in batches:
            struct = batch.column(0)
            yield [pa.RecordBatch.from_arrays(struct.flatten(), schema=pa.schema(struct.type))]

    def dump_stream(self, iterator, stream):
        """
        Override because Pandas UDFs require a START_ARROW_STREAM before the Arrow stream is sent.
        This should be sent after creating the first record batch so in case of an error, it can
        be sent back to the JVM before the Arrow stream starts.
        """
        import pyarrow as pa

        def wrap_and_init_stream():
            should_write_start_length = True
            for batch, _ in iterator:
                assert isinstance(batch, pa.RecordBatch)

                # Wrap the root struct
                struct = pa.StructArray.from_arrays(
                    batch.columns, fields=pa.struct(list(batch.schema))
                )
                batch = pa.RecordBatch.from_arrays([struct], ["_0"])

                # Write the first record batch with initialization.
                if should_write_start_length:
                    write_int(SpecialLengths.START_ARROW_STREAM, stream)
                    should_write_start_length = False
                yield batch

        return super(SedonaArrowStreamUDFSerializer, self).dump_stream(wrap_and_init_stream(), stream)


def report_times(outfile, boot, init, finish):
    write_int(SpecialLengths.TIMING_DATA, outfile)
    write_long(int(1000 * boot), outfile)
    write_long(int(1000 * init), outfile)
    write_long(int(1000 * finish), outfile)


def add_path(path):
    # worker can be used, so do not add path multiple times
    if path not in sys.path:
        # overwrite system packages
        sys.path.insert(1, path)


def read_command(serializer, file):
    command = serializer._read_with_length(file)
    if isinstance(command, Broadcast):
        command = serializer.loads(command.value)
    return command


def chain(f, g):
    """chain two functions together"""
    return lambda *a: g(f(*a))

def wrap_scalar_pandas_udf(f, return_type):
    arrow_return_type = to_arrow_type(return_type)

    def verify_result_type(result):
        if not hasattr(result, "__len__"):
            pd_type = "pandas.DataFrame" if type(return_type) == StructType else "pandas.Series"
            raise PySparkTypeError(
                error_class="UDF_RETURN_TYPE",
                message_parameters={
                    "expected": pd_type,
                    "actual": type(result).__name__,
                },
            )
        return result

    def verify_result_length(result, length):
        if len(result) != length:
            raise PySparkRuntimeError(
                error_class="SCHEMA_MISMATCH_FOR_PANDAS_UDF",
                message_parameters={
                    "expected": str(length),
                    "actual": str(len(result)),
                },
            )
        return result

    return lambda *a: (
        verify_result_length(verify_result_type(f(*a)), len(a[0])),
        arrow_return_type,
    )


def read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index):
    num_arg = read_int(infile)
    arg_offsets = [read_int(infile) for i in range(num_arg)]
    chained_func = None
    for i in range(read_int(infile)):
        f, return_type = read_command(pickleSer, infile)
        if chained_func is None:
            chained_func = f
        else:
            chained_func = chain(chained_func, f)

    if eval_type in (
            PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF,
            PythonEvalType.SQL_ARROW_BATCHED_UDF,
    ):
        func = chained_func
    else:
        # make sure StopIteration's raised in the user code are not ignored
        # when they are processed in a for loop, raise them as RuntimeError's instead
        func = fail_on_stopiteration(chained_func)

    # the last returnType will be the return type of UDF
    if eval_type == PythonEvalType.SQL_SCALAR_PANDAS_UDF:
        return arg_offsets, wrap_scalar_pandas_udf(func, return_type)
    else:
        raise ValueError("Unknown eval type: {}".format(eval_type))


# Used by SQL_GROUPED_MAP_PANDAS_UDF and SQL_SCALAR_PANDAS_UDF and SQL_ARROW_BATCHED_UDF when
# returning StructType
def assign_cols_by_name(runner_conf):
    return (
            runner_conf.get(
                "spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName", "true"
            ).lower()
            == "true"
    )


def read_udfs(pickleSer, infile, eval_type):
    runner_conf = {}

    # Load conf used for pandas_udf evaluation
    num_conf = read_int(infile)
    for i in range(num_conf):
        k = utf8_deserializer.loads(infile)
        v = utf8_deserializer.loads(infile)
        runner_conf[k] = v

    # state_object_schema = None
    # if eval_type == PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE:
    #     state_object_schema = StructType.fromJson(json.loads(utf8_deserializer.loads(infile)))

    # NOTE: if timezone is set here, that implies respectSessionTimeZone is True
    timezone = runner_conf.get("spark.sql.session.timeZone", None)
    safecheck = (
            runner_conf.get("spark.sql.execution.pandas.convertToArrowArraySafely", "false").lower()
            == "true"
    )

    df_for_struct = (
            eval_type == PythonEvalType.SQL_SCALAR_PANDAS_UDF
            or eval_type == PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF
            or eval_type == PythonEvalType.SQL_MAP_PANDAS_ITER_UDF
    )
    # Arrow-optimized Python UDF takes a struct type argument as a Row
    struct_in_pandas = (
        "row" if eval_type == PythonEvalType.SQL_ARROW_BATCHED_UDF else "dict"
    )
    ndarray_as_list = eval_type == PythonEvalType.SQL_ARROW_BATCHED_UDF
    # Arrow-optimized Python UDF uses explicit Arrow cast for type coercion
    arrow_cast = eval_type == PythonEvalType.SQL_ARROW_BATCHED_UDF

    num_udfs = read_int(infile)

    udfs = []
    for i in range(num_udfs):
        udfs.append(read_single_udf(pickleSer, infile, eval_type, runner_conf, udf_index=i))

    def mapper(a):
        result = tuple(f(*[a[o] for o in arg_offsets]) for (arg_offsets, f) in udfs)
        # In the special case of a single UDF this will return a single result rather
        # than a tuple of results; this is the format that the JVM side expects.
        if len(result) == 1:
            return result[0]
        else:
            return result

    def func(_, it):
        return map(mapper, it)

    ser = SedonaArrowStreamPandasUDFSerializer(
        timezone,
        safecheck,
        assign_cols_by_name(runner_conf),
        df_for_struct,
        struct_in_pandas,
        ndarray_as_list,
        arrow_cast,
    )

    # profiling is not supported for UDF
    return func, None, ser, ser


def main(infile, outfile):
    faulthandler_log_path = os.environ.get("PYTHON_FAULTHANDLER_DIR", None)
    try:
        if faulthandler_log_path:
            faulthandler_log_path = os.path.join(faulthandler_log_path, str(os.getpid()))
            faulthandler_log_file = open(faulthandler_log_path, "w")
            faulthandler.enable(file=faulthandler_log_file)

        boot_time = time.time()
        split_index = read_int(infile)
        if split_index == -1:  # for unit tests
            sys.exit(-1)

        version = utf8_deserializer.loads(infile)

        if version != "%d.%d" % sys.version_info[:2]:
            raise PySparkRuntimeError(
                error_class="PYTHON_VERSION_MISMATCH",
                message_parameters={
                    "worker_version": str(sys.version_info[:2]),
                    "driver_version": str(version),
                },
            )

        # read inputs only for a barrier task
        isBarrier = read_bool(infile)
        boundPort = read_int(infile)
        secret = UTF8Deserializer().loads(infile)

        # set up memory limits
        memory_limit_mb = int(os.environ.get("PYSPARK_EXECUTOR_MEMORY_MB", "-1"))
        if memory_limit_mb > 0 and has_resource_module:
            total_memory = resource.RLIMIT_AS
            try:
                (soft_limit, hard_limit) = resource.getrlimit(total_memory)
                msg = "Current mem limits: {0} of max {1}\n".format(soft_limit, hard_limit)
                print(msg, file=sys.stderr)

                # convert to bytes
                new_limit = memory_limit_mb * 1024 * 1024

                if soft_limit == resource.RLIM_INFINITY or new_limit < soft_limit:
                    msg = "Setting mem limits to {0} of max {1}\n".format(new_limit, new_limit)
                    print(msg, file=sys.stderr)
                    resource.setrlimit(total_memory, (new_limit, new_limit))

            except (resource.error, OSError, ValueError) as e:
                # not all systems support resource limits, so warn instead of failing
                lineno = (
                    getframeinfo(currentframe()).lineno + 1 if currentframe() is not None else 0
                )
                if "__file__" in globals():
                    print(
                        warnings.formatwarning(
                            "Failed to set memory limit: {0}".format(e),
                            ResourceWarning,
                            __file__,
                            lineno,
                        ),
                        file=sys.stderr,
                    )

        # initialize global state
        taskContext = None
        if isBarrier:
            taskContext = BarrierTaskContext._getOrCreate()
            BarrierTaskContext._initialize(boundPort, secret)
            # Set the task context instance here, so we can get it by TaskContext.get for
            # both TaskContext and BarrierTaskContext
            TaskContext._setTaskContext(taskContext)
        else:
            taskContext = TaskContext._getOrCreate()
        # read inputs for TaskContext info
        taskContext._stageId = read_int(infile)
        taskContext._partitionId = read_int(infile)
        taskContext._attemptNumber = read_int(infile)
        taskContext._taskAttemptId = read_long(infile)
        taskContext._cpus = read_int(infile)
        taskContext._resources = {}
        for r in range(read_int(infile)):
            key = utf8_deserializer.loads(infile)
            name = utf8_deserializer.loads(infile)
            addresses = []
            taskContext._resources = {}
            for a in range(read_int(infile)):
                addresses.append(utf8_deserializer.loads(infile))
            taskContext._resources[key] = ResourceInformation(name, addresses)

        taskContext._localProperties = dict()
        for i in range(read_int(infile)):
            k = utf8_deserializer.loads(infile)
            v = utf8_deserializer.loads(infile)
            taskContext._localProperties[k] = v

        shuffle.MemoryBytesSpilled = 0
        shuffle.DiskBytesSpilled = 0
        _accumulatorRegistry.clear()

        # fetch name of workdir
        spark_files_dir = utf8_deserializer.loads(infile)
        SparkFiles._root_directory = spark_files_dir
        SparkFiles._is_running_on_worker = True

        # fetch names of includes (*.zip and *.egg files) and construct PYTHONPATH
        add_path(spark_files_dir)  # *.py files that were added will be copied here
        num_python_includes = read_int(infile)
        for _ in range(num_python_includes):
            filename = utf8_deserializer.loads(infile)
            add_path(os.path.join(spark_files_dir, filename))

        importlib.invalidate_caches()

        # fetch names and values of broadcast variables
        needs_broadcast_decryption_server = read_bool(infile)
        num_broadcast_variables = read_int(infile)
        if needs_broadcast_decryption_server:
            # read the decrypted data from a server in the jvm
            port = read_int(infile)
            auth_secret = utf8_deserializer.loads(infile)
            (broadcast_sock_file, _) = local_connect_and_auth(port, auth_secret)

        for _ in range(num_broadcast_variables):
            bid = read_long(infile)
            if bid >= 0:
                if needs_broadcast_decryption_server:
                    read_bid = read_long(broadcast_sock_file)
                    assert read_bid == bid
                    _broadcastRegistry[bid] = Broadcast(sock_file=broadcast_sock_file)
                else:
                    path = utf8_deserializer.loads(infile)
                    _broadcastRegistry[bid] = Broadcast(path=path)

            else:
                bid = -bid - 1
                _broadcastRegistry.pop(bid)

        if needs_broadcast_decryption_server:
            broadcast_sock_file.write(b"1")
            broadcast_sock_file.close()

        _accumulatorRegistry.clear()
        eval_type = read_int(infile)
        func, profiler, deserializer, serializer = read_udfs(pickleSer, infile, eval_type)
        init_time = time.time()

        def process():
            iterator = deserializer.load_stream(infile)
            out_iter = func(split_index, iterator)
            try:
                serializer.dump_stream(out_iter, outfile)
            finally:
                if hasattr(out_iter, "close"):
                    out_iter.close()

        if profiler:
            profiler.profile(process)
        else:
            process()

        # Reset task context to None. This is a guard code to avoid residual context when worker
        # reuse.
        TaskContext._setTaskContext(None)
        BarrierTaskContext._setTaskContext(None)
    except BaseException as e:
        try:
            exc_info = None
            if os.environ.get("SPARK_SIMPLIFIED_TRACEBACK", False):
                tb = try_simplify_traceback(sys.exc_info()[-1])
                if tb is not None:
                    e.__cause__ = None
                    exc_info = "".join(traceback.format_exception(type(e), e, tb))
            if exc_info is None:
                exc_info = traceback.format_exc()

            write_int(SpecialLengths.PYTHON_EXCEPTION_THROWN, outfile)
            write_with_length(exc_info.encode("utf-8"), outfile)
        except IOError:
            # JVM close the socket
            pass
        except BaseException:
            # Write the error to stderr if it happened while serializing
            print("PySpark worker failed with exception:", file=sys.stderr)
            print(traceback.format_exc(), file=sys.stderr)
        sys.exit(-1)
    finally:
        if faulthandler_log_path:
            faulthandler.disable()
            faulthandler_log_file.close()
            os.remove(faulthandler_log_path)
    finish_time = time.time()
    report_times(outfile, boot_time, init_time, finish_time)
    write_long(shuffle.MemoryBytesSpilled, outfile)
    write_long(shuffle.DiskBytesSpilled, outfile)

    # Mark the beginning of the accumulators section of the output
    write_int(SpecialLengths.END_OF_DATA_SECTION, outfile)
    write_int(len(_accumulatorRegistry), outfile)
    for (aid, accum) in _accumulatorRegistry.items():
        pickleSer._write_with_length((aid, accum._value), outfile)

    # check end of stream
    if read_int(infile) == SpecialLengths.END_OF_STREAM:
        write_int(SpecialLengths.END_OF_STREAM, outfile)
    else:
        # write a different value to tell JVM to not reuse this worker
        write_int(SpecialLengths.END_OF_DATA_SECTION, outfile)
        sys.exit(-1)


if __name__ == "__main__":
    # Read information about how to connect back to the JVM from the environment.
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    (sock_file, _) = local_connect_and_auth(java_port, auth_secret)
    # TODO: Remove the following two lines and use `Process.pid()` when we drop JDK 8.
    write_int(os.getpid(), sock_file)
    sock_file.flush()
    main(sock_file, sock_file)


class GeoArrowLoader:
    pass
