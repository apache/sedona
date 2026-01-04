import importlib
import os
import sys
import time

import sedonadb
from pyspark import TaskContext, shuffle, SparkFiles
from pyspark.errors import PySparkRuntimeError
from pyspark.java_gateway import local_connect_and_auth
from pyspark.resource import ResourceInformation
from pyspark.serializers import read_int, UTF8Deserializer, read_bool, read_long, CPickleSerializer, write_int, \
    write_long, SpecialLengths

from sedona.spark.worker.serde import SedonaDBSerializer
from sedona.spark.worker.udf_info import UDFInfo


def apply_iterator(db, iterator, udf_info: UDFInfo):
    i = 0
    for df in iterator:
        i+=1
        table_name = f"output_table_{i}"
        df.to_view(table_name)

        function_call_sql = udf_info.get_function_call_sql(table_name)

        df_out = db.sql(function_call_sql)
        df_out.to_view(f"view_{i}")
        at = df_out.to_arrow_table()
        batches = at.combine_chunks().to_batches()

        for batch in batches:
            yield batch


def check_python_version(utf_serde: UTF8Deserializer, infile) -> str:
    version = utf_serde.loads(infile)

    python_major, python_minor = sys.version_info[:2]

    if version != f"{python_major}.{python_minor}":
        raise PySparkRuntimeError(
            error_class="PYTHON_VERSION_MISMATCH",
            message_parameters={
                "worker_version": str(sys.version_info[:2]),
                "driver_version": str(version),
            },
        )

    return version

def check_barrier_flag(infile):
    is_barrier = read_bool(infile)
    bound_port = read_int(infile)
    secret = UTF8Deserializer().loads(infile)

    if is_barrier:
        raise PySparkRuntimeError(
            error_class="BARRIER_MODE_NOT_SUPPORTED",
            message_parameters={
                "worker_version": str(sys.version_info[:2]),
                "message": "Barrier mode is not supported by SedonaDB vectorized functions.",
            },
        )

    return is_barrier

def assign_task_context(utf_serde: UTF8Deserializer, infile):
    stage_id = read_int(infile)
    partition_id = read_int(infile)
    attempt_number = read_long(infile)
    task_attempt_id = read_int(infile)
    cpus = read_int(infile)

    task_context = TaskContext._getOrCreate()
    task_context._stage_id = stage_id
    task_context._partition_id = partition_id
    task_context._attempt_number = attempt_number
    task_context._task_attempt_id = task_attempt_id
    task_context._cpus = cpus

    for r in range(read_int(infile)):
        key = utf_serde.loads(infile)
        name = utf_serde.loads(infile)
        addresses = []
        task_context._resources = {}
        for a in range(read_int(infile)):
            addresses.append(utf_serde.loads(infile))
        task_context._resources[key] = ResourceInformation(name, addresses)

    task_context._localProperties = dict()
    for i in range(read_int(infile)):
        k = utf_serde.loads(infile)
        v = utf_serde.loads(infile)
        task_context._localProperties[k] = v

    return task_context

def resolve_python_path(utf_serde: UTF8Deserializer, infile):
    def add_path(path: str):
        # worker can be used, so do not add path multiple times
        if path not in sys.path:
            # overwrite system packages
            sys.path.insert(1, path)

    spark_files_dir = utf_serde.loads(infile)

    SparkFiles._root_directory = spark_files_dir
    SparkFiles._is_running_on_worker = True

    add_path(spark_files_dir)  # *.py files that were added will be copied here
    num_python_includes = read_int(infile)
    for _ in range(num_python_includes):
        filename = utf_serde.loads(infile)
        add_path(os.path.join(spark_files_dir, filename))

    importlib.invalidate_caches()


def check_broadcast_variables(infile):
    needs_broadcast_decryption_server = read_bool(infile)
    num_broadcast_variables = read_int(infile)

    if needs_broadcast_decryption_server or num_broadcast_variables > 0:
        raise PySparkRuntimeError(
            error_class="BROADCAST_VARS_NOT_SUPPORTED",
            message_parameters={
                "worker_version": str(sys.version_info[:2]),
                "message": "Broadcast variables are not supported by SedonaDB vectorized functions.",
            },
        )

def get_runner_conf(utf_serde: UTF8Deserializer, infile):
    runner_conf = {}
    num_conf = read_int(infile)
    for i in range(num_conf):
        k = utf_serde.loads(infile)
        v = utf_serde.loads(infile)
        runner_conf[k] = v
    return runner_conf


def read_command(serializer, infile):
    command = serializer._read_with_length(infile)
    return command

def read_udf(infile, pickle_ser) -> UDFInfo:
    num_arg = read_int(infile)
    arg_offsets = [read_int(infile) for i in range(num_arg)]

    function = None
    return_type = None

    for i in range(read_int(infile)):
        function, return_type = read_command(pickle_ser, infile)

    sedona_db_udf_expression = function()

    return UDFInfo(
        arg_offsets=arg_offsets,
        function=sedona_db_udf_expression,
        return_type=return_type,
        name=sedona_db_udf_expression._name,
        geom_offsets=[0]
    )

def register_sedona_db_udf(infile, pickle_ser) -> UDFInfo:
    num_udfs = read_int(infile)

    udf = None
    for _ in range(num_udfs):
        udf = read_udf(infile, pickle_ser)
        # Here we would register the UDF with SedonaDB's internal context


    return udf


def report_times(outfile, boot, init, finish):
    write_int(SpecialLengths.TIMING_DATA, outfile)
    write_long(int(1000 * boot), outfile)
    write_long(int(1000 * init), outfile)
    write_long(int(1000 * finish), outfile)


def write_statistics(infile, outfile, boot_time, init_time) -> None:
    TaskContext._setTaskContext(None)
    finish_time = time.time()
    report_times(outfile, boot_time, init_time, finish_time)
    write_long(shuffle.MemoryBytesSpilled, outfile)
    write_long(shuffle.DiskBytesSpilled, outfile)

    write_int(SpecialLengths.END_OF_DATA_SECTION, outfile)

    if read_int(infile) == SpecialLengths.END_OF_STREAM:
        write_int(SpecialLengths.END_OF_STREAM, outfile)
        outfile.flush()
    else:
        write_int(SpecialLengths.END_OF_DATA_SECTION, outfile)
        outfile.flush()
        sys.exit(-1)


def main(infile, outfile):
    boot_time = time.time()
    sedona_db = sedonadb.connect()
    #
    utf8_deserializer = UTF8Deserializer()
    pickle_ser = CPickleSerializer()

    split_index = read_int(infile)
    #
    check_python_version(utf8_deserializer, infile)
    #
    check_barrier_flag(infile)

    task_context = assign_task_context(utf_serde=utf8_deserializer, infile=infile)
    shuffle.MemoryBytesSpilled = 0
    shuffle.DiskBytesSpilled = 0

    resolve_python_path(utf8_deserializer, infile)
    #
    check_broadcast_variables(infile)

    eval_type = read_int(infile)

    runner_conf = get_runner_conf(utf8_deserializer, infile)

    udf = register_sedona_db_udf(infile, pickle_ser)

    sedona_db.register_udf(udf.function)
    init_time = time.time()

    serde = SedonaDBSerializer(
        timezone=runner_conf.get("spark.sql.session.timeZone", "UTC"),
        safecheck=False,
        db=sedona_db,
        udf_info=udf
    )

    number_of_geometries = read_int(infile)
    geom_offsets = {}
    for i in range(number_of_geometries):
        geom_index = read_int(infile)
        geom_srid = read_int(infile)

        geom_offsets[geom_index] = geom_srid

    udf.geom_offsets = geom_offsets

    iterator = serde.load_stream(infile)
    out_iterator = apply_iterator(db=sedona_db, iterator=iterator, udf_info=udf)

    serde.dump_stream(out_iterator, outfile)

    write_statistics(
        infile, outfile, boot_time=boot_time, init_time=init_time
    )


if __name__ == "__main__":
    # add file handler
    auth_secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    (sock_file, sc) = local_connect_and_auth(java_port, auth_secret)

    write_int(os.getpid(), sock_file)
    sock_file.flush()

    main(sock_file, sock_file)
