from pyspark.sql.pandas.serializers import ArrowStreamPandasSerializer, ArrowStreamSerializer
from pyspark.errors import PySparkTypeError, PySparkValueError
import struct

from pyspark.serializers import write_int

def write_int(value, stream):
    stream.write(struct.pack("!i", value))

class SpecialLengths:
    START_ARROW_STREAM = -6


class SedonaArrowStreamPandasUDFSerializer(ArrowStreamPandasSerializer):
    def __init__(self, timezone, safecheck, assign_cols_by_name):
        super(SedonaArrowStreamPandasUDFSerializer, self).__init__(timezone, safecheck)
        self._assign_cols_by_name = assign_cols_by_name

    def load_stream(self, stream):
        import geoarrow.pyarrow as ga
        import pyarrow as pa

        batches = super(ArrowStreamPandasSerializer, self).load_stream(stream)
        for batch in batches:
            table = pa.Table.from_batches(batches=[batch])
            data = []

            for c in table.itercolumns():
                meta = table.schema.field(c._name).metadata
                if meta and meta[b"ARROW:extension:name"] == b'geoarrow.wkb':
                    data.append(ga.to_geopandas(c))
                    continue

                data.append(self.arrow_to_pandas(c))

            yield data

    def _create_batch(self, series):
        import pyarrow as pa

        series = ((s, None) if not isinstance(s, (list, tuple)) else s for s in series)

        arrs = []
        for s, t in series:
            arrs.append(self._create_array(s, t))

        return pa.RecordBatch.from_arrays(arrs, ["_%d" % i for i in range(len(arrs))])

    def _create_array(self, series, arrow_type):
        import pyarrow as pa
        import geopandas as gpd

        if hasattr(series.array, "__arrow_array__"):
            mask = None
        else:
            mask = series.isnull()

        try:
            if isinstance(series, gpd.GeoSeries):
                import geoarrow.pyarrow as ga
                # If the series is a GeoSeries, convert it to an Arrow array using geoarrow
                return ga.array(series)

            array = pa.Array.from_pandas(series, mask=mask, type=arrow_type)
            return array
        except TypeError as e:
            error_msg = (
                "Exception thrown when converting pandas.Series (%s) "
                "with name '%s' to Arrow Array (%s)."
            )
            raise PySparkTypeError(error_msg % (series.dtype, series.name, arrow_type)) from e
        except ValueError as e:
            error_msg = (
                "Exception thrown when converting pandas.Series (%s) "
                "with name '%s' to Arrow Array (%s)."
            )
            raise PySparkValueError(error_msg % (series.dtype, series.name, arrow_type)) from e

    def dump_stream(self, iterator, stream):
        """
        Override because Pandas UDFs require a START_ARROW_STREAM before the Arrow stream is sent.
        This should be sent after creating the first record batch so in case of an error, it can
        be sent back to the JVM before the Arrow stream starts.
        """

        def init_stream_yield_batches():
            should_write_start_length = True
            for series in iterator:
                batch = self._create_batch(series)
                if should_write_start_length:
                    write_int(SpecialLengths.START_ARROW_STREAM, stream)
                    should_write_start_length = False
                yield batch

        return ArrowStreamSerializer.dump_stream(self, init_stream_yield_batches(), stream)

    def __repr__(self):
        return "ArrowStreamPandasUDFSerializer"


