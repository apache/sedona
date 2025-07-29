import logging

import pandas as pd
from pyspark.sql.pandas.serializers import ArrowStreamPandasSerializer, ArrowStreamSerializer
from pyspark.errors import PySparkTypeError, PySparkValueError
import struct
from pyspark.sql.pandas.types import (
    from_arrow_type,
    to_arrow_type,
    _create_converter_from_pandas,
    _create_converter_to_pandas,
)

def write_int(value, stream):
    stream.write(struct.pack("!i", value))

class SpecialLengths:
    END_OF_DATA_SECTION = -1
    PYTHON_EXCEPTION_THROWN = -2
    TIMING_DATA = -3
    END_OF_STREAM = -4
    NULL = -5
    START_ARROW_STREAM = -6


class SedonaArrowStreamPandasUDFSerializer(ArrowStreamPandasSerializer):
    """
    Serializer used by Python worker to evaluate Pandas UDFs
    """

    def __init__(
            self,
            timezone,
            safecheck,
            assign_cols_by_name,
            df_for_struct=False,
            struct_in_pandas="dict",
            ndarray_as_list=False,
            arrow_cast=False,
    ):
        super(SedonaArrowStreamPandasUDFSerializer, self).__init__(timezone, safecheck)
        self._assign_cols_by_name = assign_cols_by_name
        self._df_for_struct = df_for_struct
        self._struct_in_pandas = struct_in_pandas
        self._ndarray_as_list = ndarray_as_list
        self._arrow_cast = arrow_cast

    def load_stream(self, stream):
        """
        Deserialize ArrowRecordBatches to an Arrow table and return as a list of pandas.Series.
        """
        import geoarrow.pyarrow as ga
        batches = super(ArrowStreamPandasSerializer, self).load_stream(stream)
        import pyarrow as pa
        for batch in batches:
            yield [ga.to_geopandas(c) for c in pa.Table.from_batches([batch]).itercolumns()]

    def _create_batch(self, series):
        """
        Create an Arrow record batch from the given pandas.Series pandas.DataFrame
        or list of Series or DataFrame, with optional type.

        Parameters
        ----------
        series : pandas.Series or pandas.DataFrame or list
            A single series or dataframe, list of series or dataframe,
            or list of (series or dataframe, arrow_type)

        Returns
        -------
        pyarrow.RecordBatch
            Arrow RecordBatch
        """
        import pyarrow as pa

        # Make input conform to [(series1, type1), (series2, type2), ...]
        if not isinstance(series, (list, tuple)) or (
                len(series) == 2 and isinstance(series[1], pa.DataType)
        ):
            series = [series]
        series = ((s, None) if not isinstance(s, (list, tuple)) else s for s in series)

        arrs = []
        for s, t in series:
            arrs.append(self._create_array(s, t, arrow_cast=self._arrow_cast))

        return pa.RecordBatch.from_arrays(arrs, ["_%d" % i for i in range(len(arrs))])

    def _create_array(self, series, arrow_type, spark_type=None, arrow_cast=False):
        """
        Create an Arrow Array from the given pandas.Series and optional type.

        Parameters
        ----------
        series : pandas.Series
            A single series
        arrow_type : pyarrow.DataType, optional
            If None, pyarrow's inferred type will be used
        spark_type : DataType, optional
            If None, spark type converted from arrow_type will be used
        arrow_cast: bool, optional
            Whether to apply Arrow casting when the user-specified return type mismatches the
            actual return values.

        Returns
        -------
        pyarrow.Array
        """
        import pyarrow as pa
        from pandas.api.types import is_categorical_dtype
        if is_categorical_dtype(series.dtype):
            series = series.astype(series.dtypes.categories.dtype)
        if arrow_type is not None:
            dt = spark_type or from_arrow_type(arrow_type, prefer_timestamp_ntz=True)
            # TODO(SPARK-43579): cache the converter for reuse
            conv = _create_converter_from_pandas(
                dt, timezone=self._timezone, error_on_duplicated_field_names=False
            )
            series = conv(series)

        if hasattr(series.array, "__arrow_array__"):
            mask = None
        else:
            mask = series.isnull()
        try:
            try:
                import geopandas as gpd
                if isinstance(series, gpd.GeoSeries):
                    import geoarrow.pyarrow as ga
                    # If the series is a GeoSeries, convert it to an Arrow array using geoarrow
                    return ga.array(series)

                array = pa.Array.from_pandas(
                    series, mask=mask, type=arrow_type, safe=self._safecheck
                )

                return array
            except pa.lib.ArrowInvalid:
                if arrow_cast:
                    return pa.Array.from_pandas(series, mask=mask).cast(
                        target_type=arrow_type, safe=self._safecheck
                    )
                else:
                    raise
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
            if self._safecheck:
                error_msg = error_msg + (
                    " It can be caused by overflows or other "
                    "unsafe conversions warned by Arrow. Arrow safe type check "
                    "can be disabled by using SQL config "
                    "`spark.sql.execution.pandas.convertToArrowArraySafely`."
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

from pyspark.serializers import Serializer, write_int

