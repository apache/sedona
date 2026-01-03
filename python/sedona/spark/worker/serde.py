import socket

from pyspark.serializers import write_int, SpecialLengths
from pyspark.sql.pandas.serializers import ArrowStreamPandasSerializer

from sedona.spark.worker.udf_info import UDFInfo


def read_available(buf, chunk=4096):
    # buf.raw._sock.settimeout(0.01)   # non-blocking-ish
    data = bytearray()
    index = 0
    while True:
        index+=1
        try:
            chunk_bytes = buf.read(chunk)
        except socket.timeout:
            break

        if not chunk_bytes and index > 10:
            break

        data.extend(chunk_bytes)

    return bytes(data)

class SedonaDBSerializer(ArrowStreamPandasSerializer):
    def __init__(self, timezone, safecheck, db, udf_info: UDFInfo):
        super(SedonaDBSerializer, self).__init__(timezone, safecheck)
        self.db = db
        self.udf_info = udf_info

    def load_stream(self, stream):
        import pyarrow as pa

        batches = super(ArrowStreamPandasSerializer, self).load_stream(stream)
        index = 0
        for batch in batches:
            table = pa.Table.from_batches(batches=[batch])
            import pyarrow as pa
            df = self.db.create_data_frame(table)
            table_name = f"my_table_{index}"

            df.to_view(table_name)

            sql_expression = self.udf_info.sedona_db_transformation_expr(table_name)

            index += 1

            yield self.db.sql(sql_expression)

    def arrow_dump_stream(self, iterator, stream):
        import pyarrow as pa

        writer = None
        try:
            for batch in iterator:
                if writer is None:
                    writer = pa.RecordBatchStreamWriter(stream, batch.schema)
                writer.write_batch(batch)
                # stream.flush()
        finally:
            if writer is not None:
                writer.close()

    def dump_stream(self, iterator, stream):
        """
        Override because Pandas UDFs require a START_ARROW_STREAM before the Arrow stream is sent.
        This should be sent after creating the first record batch so in case of an error, it can
        be sent back to the JVM before the Arrow stream starts.
        """

        def init_stream_yield_batches():
            should_write_start_length = True
            for batch in iterator:
                if should_write_start_length:
                    write_int(SpecialLengths.START_ARROW_STREAM, stream)
                    should_write_start_length = False

                yield batch

        return self.arrow_dump_stream(init_stream_yield_batches(), stream)
