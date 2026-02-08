# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pyspark.serializers import write_int, SpecialLengths
from pyspark.sql.pandas.serializers import ArrowStreamPandasSerializer

from sedona.spark.worker.udf_info import UDFInfo


class SedonaDBSerializer(ArrowStreamPandasSerializer):
    def __init__(self, timezone, safecheck, db, udf_info: UDFInfo, cast_to_wkb=False):
        super().__init__(timezone, safecheck)
        self.db = db
        self.udf_info = udf_info
        self.cast_to_wkb = cast_to_wkb

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

            sql_expression = self.udf_info.sedona_db_transformation_expr(
                table_name, self.cast_to_wkb
            )

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
        finally:
            if writer is not None:
                writer.close()

    def dump_stream(self, iterator, stream):
        def init_stream_yield_batches():
            should_write_start_length = True
            for batch in iterator:
                if should_write_start_length:
                    write_int(SpecialLengths.START_ARROW_STREAM, stream)
                    should_write_start_length = False

                yield batch

        return self.arrow_dump_stream(init_stream_yield_batches(), stream)
